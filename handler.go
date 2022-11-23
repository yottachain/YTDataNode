package node

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"

	//"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multiaddr"

	//"github.com/tecbot/gorocksdb"

	//"github.com/tecbot/gorocksdb"
	"github.com/yottachain/YTDataNode/TokenPool"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/slicecompare"
	"github.com/yottachain/YTDataNode/statistics"
	yhservice "github.com/yottachain/YTHost/service"

	"github.com/yottachain/YTDataNode/spotCheck"

	"github.com/yottachain/YTDataNode/message"

	"github.com/golang/protobuf/proto"
	. "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTFS/common"
)

var disableWrite = false

// WriteHandler 写入处理器
type WriteHandler struct {
	StorageNode
	RequestQueue chan *wRequest
	TmpDB        *CompDB
	seq          uint64
	isWriteSleep bool
}

func NewWriteHandler(sn StorageNode) *WriteHandler {
	TDB := sn.GetCompareDb()

	seq, err := slicecompare.GetSeqFromDb(TDB, slicecompare.Seqkey)
	if err != nil {
		log.Println("[slicecompare] get seq from compare_db error")
		return nil
	}

	return &WriteHandler{
		sn,
		make(chan *wRequest, 1000),
		TDB,
		seq,
		false,
	}
}

type YTres struct {
	seq uint64
	//hash []byte
}

type wRequest struct {
	Key   common.IndexTableKey
	Data  []byte
	ytres YTres
	err   chan error
}

func (wh *WriteHandler) push(ctx context.Context, key common.IndexTableKey, data []byte) (YTres, error) {
	rq := &wRequest{
		Key:  key,
		Data: data,
		err:  make(chan error),
	}

	select {
	case wh.RequestQueue <- rq:
		log.Println("[task]push task success")
	default:
		//return fmt.Errorf("task busy", len(wh.RequestQueue))
	}
	select {
	case err := <-rq.err:
		return rq.ytres, err
	case <-ctx.Done():
		var ytres YTres
		err := fmt.Errorf("get error time out")
		return ytres, err
	}
}

func (wh *WriteHandler) batchWrite(number int) {
	totalTime := time.Now()
	var firstShardHash []byte

	var err, err2 error
	rqmap := make(map[common.IndexTableKey][]byte, number)
	rqs := make([]*wRequest, number)
	hashkey := make([][]byte, number)

	startTime := time.Now()

	for i := 0; i < number; i++ {
		select {
		case rq := <-wh.RequestQueue:
			atomic.AddUint64(&wh.seq, 1)
			rq.ytres.seq = wh.seq
			rqmap[rq.Key] = rq.Data
			rqs[i] = rq
			hashkey[i] = rq.Key.Hsh[:]

			log.Printf("[test] recive shard %s  shard id %d seq %d\n",
				base64.StdEncoding.EncodeToString(hashkey[i]), int64(rq.Key.Id),
				rq.ytres.seq)

			err = slicecompare.PutKSeqToDb(wh.seq, hashkey[i], wh.TmpDB)
			if err != nil {
				log.Println("[slicecompare] put to compare_db error:", err.Error(),
					"compare_seq:", wh.seq, "hash:", base58.Encode(hashkey[i]))
				goto OUT
			}
		default:
			continue
		}
	}

	for key, _ := range rqmap {
		firstShardHash = key.Hsh[:]
	}
	log.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, batchWrite-slicecompare.PutKSeqToDb use [%f]\n", base58.Encode(firstShardHash), len(rqmap), time.Now().Sub(startTime).Seconds())

	log.Printf("[ytfs]flush start:%d\n", number)
	startTime = time.Now()
	_, err = wh.putShard(rqmap)
	log.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, batchWrite-wh.putShard use [%f]\n", base58.Encode(firstShardHash), len(rqmap), time.Now().Sub(startTime).Seconds())
	if err == nil {
		log.Printf("[ytfs]flush sucess:%d wh.seq: %d\n", number, wh.seq)
	} else if !strings.Contains(err.Error(), "read ytfs time out") {
		if _, ok := os.LookupEnv("ytfs_dev"); !ok {
			log.Printf("[ytfs]flush failure:%s\n", err.Error())
			statistics.DefaultStat.Lock()
			statistics.DefaultStat.YTFSErrorCount = statistics.DefaultStat.YTFSErrorCount + 1
			if statistics.DefaultStat.YTFSErrorCount > 100 {
				disableWrite = true
			}
			statistics.DefaultStat.Unlock()
		}
	}

	//wh.seq = wh.seq + uint64(number)
	startTime = time.Now()
	err2 = slicecompare.PutVSeqToDb(wh.seq, []byte(slicecompare.Seqkey), wh.TmpDB)
	log.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, batchWrite-slicecompare.PutVSeqToDb use [%f]\n", base58.Encode(firstShardHash), len(rqmap), time.Now().Sub(startTime).Seconds())

	if err2 != nil {
		fmt.Println("[slicecompare] PutVSeqToDb error:", err)
	}
OUT:
	startTime = time.Now()
	for _, rq := range rqs {
		select {
		case rq.err <- err:
		default:
			continue
		}
	}
	log.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, batchWrite-notify use [%f]\n", base58.Encode(firstShardHash), len(rqmap), time.Now().Sub(startTime).Seconds())
	log.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, batchWrite total use [%f]\n", base58.Encode(firstShardHash), len(rqmap), time.Now().Sub(totalTime).Seconds())

}

func (wh *WriteHandler) Run() {
	go TokenPool.Utp().FillToken()
	go TokenPool.Dtp().FillToken()
	go func() {
		var flushInterval time.Duration = time.Millisecond * 10
		for {
			select {
			case <-time.After(flushInterval):
				if n := len(wh.RequestQueue); n > 0 {
					wh.batchWrite(n)
				}
			}
		}
	}()
}

func (wh *WriteHandler) GetMaxSpace() uint64 {
	return wh.Config().AllocSpace/uint64(wh.YTFS().Meta().DataBlockSize) - 10
}

func (wh *WriteHandler) GenAllocID(id peer.ID) (string, int) {
	var xtp = TokenPool.Utp()
	var level int32 = 0

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Gconfig.TokenWait)*time.Millisecond)
	if config.Gconfig.TokenWait == 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	}
	defer cancel()

	tk, err := xtp.Get(ctx, id, level)

	allocID := ""
	res := 0

	// 如果 剩余空间不足10个分片停止发放token
	if wh.GetMaxSpace() <= (wh.YTFS().Len()) {
		tk = nil
		res = 1
		err = fmt.Errorf("YTFS： space is not enough")
	}

	if err != nil {
		if res == 0 {
			res = 2
		}
	} else {
		allocID = tk.String()
	}
	// 如果token为空 返回 假
	if allocID == "" {
		//time.Sleep(time.Duration(config.Gconfig.TokenReturnWait) * time.Millisecond)
	} else {
		atomic.AddInt64(&statistics.DefaultStat.SentTokenNum, 1)
		atomic.AddInt64(&statistics.DefaultStat.RXToken, 1)
	}

	return allocID, res

}

func (wh *WriteHandler) GetToken(data []byte, id peer.ID, ip []multiaddr.Multiaddr) []byte {
	var GTMsg message.NodeCapacityRequest
	var xtp = TokenPool.Utp()
	var tokenType = "upload"
	// 目前全都为0
	var level int32 = 0
	err := proto.Unmarshal(data, &GTMsg)

	if err == nil {
		switch GTMsg.RequestMsgID {
		case message.MsgIDDownloadShardRequest.Value() + 1, message.MsgIDMultiTaskDescription.Value() + 1:
			xtp = TokenPool.Dtp()
			atomic.AddInt64(&statistics.DefaultStat.TXRequestToken, 1)
			tokenType = "download"
		case message.MsgIDDownloadShardRequest.Value(), message.MsgIDMultiTaskDescription.Value():
			log.Println("get download token ", id.String(), GTMsg.RequestMsgID)
			return nil
		case message.MsgIDTestGetBlock.Value():
			xtp = TokenPool.Dtp()
			level = 0
			tokenType = "test"
		case message.MsgIDTestGetBlock.Value() + 1:
			xtp = TokenPool.Utp()
			level = 0
			tokenType = "testUpload"
		default:
			atomic.AddInt64(&statistics.DefaultStat.RXRequestToken, 1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Gconfig.TokenWait)*time.Millisecond)
	if config.Gconfig.TokenWait == 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	}
	defer cancel()

	tk, err := xtp.Get(ctx, id, level)

	// 如果 剩余空间不足10个分片停止发放token
	if wh.GetMaxSpace() <= (wh.YTFS().Len()) {
		tk = nil
		err = fmt.Errorf("YTFS： space is not enough")
	}

	var res message.NodeCapacityResponse
	res.Writable = true
	if err != nil {
		//fmt.Println("[get token] error:", err.Error())
		res.Writable = false
	} else {
		res.AllocId = tk.String()
	}
	// 如果token为空 返回 假
	if res.AllocId == "" {
		res.Writable = false
		time.Sleep(time.Duration(config.Gconfig.TokenReturnWait) * time.Millisecond)
	} else {
		switch tokenType {
		case "upload":
			atomic.AddInt64(&statistics.DefaultStat.SentTokenNum, 1)
			atomic.AddInt64(&statistics.DefaultStat.RXToken, 1)
		case "download":
			atomic.AddInt64(&statistics.DefaultStat.SentDownloadTokenNum, 1)
			atomic.AddInt64(&statistics.DefaultStat.TXToken, 1)
		case "test":
			statistics.DefaultStat.TXTest.AddCount()
		case "testUpload":
			statistics.DefaultStat.RXTest.AddCount()
		}
	}
	resbuf, _ := proto.Marshal(&res)

	return append(message.MsgIDNodeCapacityResponse.Bytes(), resbuf...)
}

// Handle 获取回调处理函数
func (wh *WriteHandler) Handle(msgData []byte, head yhservice.Head) []byte {
	var ytres YTres
	startTime := time.Now()
	var msg message.UploadShardRequest
	proto.Unmarshal(msgData, &msg)

	log.Printf("shard [VHF:%s] need save \n", base58.Encode(msg.VHF))
	log.Printf("[uploadslice] BPDID=%s, BPDSIGN=%s, USERSIGN=%s \n",
		msg.BPDID, base58.Encode(msg.BPDSIGN), base58.Encode(msg.USERSIGN))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	ctx = context.WithValue(ctx, "pid", head.RemotePeerID)
	defer cancel()
	var resCode int32
	// 判断剩余空间
	if wh.GetMaxSpace() >= (wh.YTFS().Len()) {
		resCode, ytres = wh.saveSlice(ctx, msg)
	} else {
		// 剩余空间不足
		resCode = 106
	}

	if resCode != 0 {
		log.Printf("shard [VHF:%s] write failed [%f]\n",
			base58.Encode(msg.VHF), time.Now().Sub(startTime).Seconds())
	} else {
		atomic.AddInt64(&statistics.DefaultStat.RXSuccess, 1)
		log.Printf("[slicecompare] origin  shard [VHF:%s] write success [%f]\n",
			base58.Encode(msg.VHF), time.Now().Sub(startTime).Seconds())
	}

	res2client, err := msg.GetResponseToClientByCode(resCode, ytres.seq, wh.Config().PrivKeyString())
	if err != nil {
		log.Println("Get res code 2 client fail:", err)
		log.Printf("shard [VHF:%s] return client failed [%f]\n",
			base58.Encode(msg.VHF), time.Now().Sub(startTime).Seconds())
	}
	return res2client
}

func (wh *WriteHandler) putShard(batch map[common.IndexTableKey][]byte) (map[common.IndexTableKey]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(config.Gconfig.DiskTimeout))
	defer cancel()

	var errorC = make(chan error)
	var success = make(chan map[common.IndexTableKey]byte)

	go func() {
		var firstShardHash []byte
		for key, _ := range batch {
			firstShardHash = key.Hsh[:]
		}
		startTime := time.Now()
		res, err := wh.YTFS().BatchPut(batch)
		log.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, batchWrite-wh.putShard-YTFS().BatchPut use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())
		if err != nil {
			errorC <- err
		}
		success <- res
	}()

	select {
	case err := <-errorC:
		return nil, err
	case res := <-success:
		return res, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("ytfs put time out")
	}
}

func (wh *WriteHandler) saveSlice(ctx context.Context, msg message.UploadShardRequest) (int32, YTres) {
	var ytres YTres
	var needCheckTk = true
	if config.Gconfig.NullWrite {
		return 0, ytres
	}
	/*
			sleepTimes := 0
		    for {
		    	if !wh.isWriteSleep {
		        	break
		        }
				sleepTimes++
				time.Sleep(time.Millisecond * 100)
				if sleepTimes >= 50 {
					break
				}
		    }*/
	log.Printf("[task pool][%s]check allocID[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
	if msg.AllocId == "" {
		if config.Gconfig.NeedToken {
			return 105, ytres
		}
		tk := TokenPool.NewToken()
		pid := ctx.Value("pid").(peer.ID)
		tk.PID = pid
		msg.AllocId = tk.String()
		needCheckTk = false
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
	}
	tk, err := TokenPool.NewTokenFromString(msg.AllocId)
	if err != nil {
		// buys
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		log.Println("token check error：", err.Error())
		pid := ctx.Value("pid").(peer.ID)

		wh.Host().ClientStore().Close(pid)
		recover()
		return 105, ytres
	}
	if needCheckTk && !TokenPool.Utp().Check(tk) {
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		log.Println("token check fail：", time.Now().Sub(tk.Tm).Milliseconds())
		return 105, ytres
	}
	atomic.AddInt64(&statistics.DefaultStat.RXRequest, 1)
	TokenPool.Utp().NetLatency.Add(time.Now().Sub(tk.Tm))

	//1. 验证BP签名
	//if ok, err := msg.VerifyBPSIGN(
	//	// 获取BP公钥
	//	host.PubKey(wh.Host().Peerstore().PubKey(wh.GetBP(msg.BPDID))),
	//	wh.Host().ID().Pretty(),
	//); err != nil || ok == false {
	//	log.Println(fmt.Errorf("Verify BPSIGN fail:%s", err))
	//	return 100
	//}

	// 2. 验证数据Hash
	if msg.VerifyVHF(msg.DAT) == false {
		log.Println(fmt.Errorf("Verify VHF fail"))
		return 100, ytres
	}

	// 3. 将数据写入YTFS-disk
	var indexKey common.Hash
	copy(indexKey[:], msg.VHF[0:16])
	var IKey common.IndexTableKey
	IKey.Hsh = indexKey
	IKey.Id = common.HashId(msg.HASHID)

	putStartTime := time.Now()
	ytres, err = wh.push(ctx, IKey, msg.DAT)
	if err != nil {
		if err.Error() == "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value" {
			return 102, ytres
		}
		log.Println("数据写入错误error:", err.Error())
		if strings.Contains(err.Error(), "ytfs put time out") {
			wh.isWriteSleep = true
			log.Println("流控开启模式进入---")
			return 112, ytres
		}
		if strings.Contains(err.Error(), "no space") {
			atomic.AddInt64(&statistics.DefaultStat.NoSpaceError, 1)
		}
		if strings.Contains(err.Error(), "input/output error") {
			atomic.AddInt64(&statistics.DefaultStat.MediumError, 1)
		}
		if strings.Contains(err.Error(), "Range is full") {
			atomic.AddInt64(&statistics.DefaultStat.RangeFullError, 1)
		}
		return 101, ytres
	}
	log.Println("return msg", 0)
	wh.isWriteSleep = false
	diskltc := time.Now().Sub(putStartTime)
	// 成功计数
	TokenPool.Utp().DiskLatency.Add(diskltc)
	if diskltc > time.Second*10 {
		log.Printf("[disklatency] %f s\n", diskltc.Seconds())
	}

	TokenPool.Utp().Delete(tk)

	return 0, ytres
}

// DownloadHandler 下载处理器
type DownloadHandler struct {
	StorageNode
}

// Handle 获取处理器
func (dh *DownloadHandler) Handle(msgData []byte, pid peer.ID) ([]byte, error) {
	var msg message.DownloadShardRequest
	var err error
	var resData []byte
	res := message.DownloadShardResponse{}

	//var indexKey [16]byte
	var indexKey common.IndexTableKey
	err = proto.Unmarshal(msgData, &msg)
	if err != nil {
		fmt.Println("Unmarshal error:", err)
		return nil, err
	}

	log.Println("[download_debugtime] A start get vhf:",
		base58.Encode(msg.VHF), "peer id:", pid.Pretty())
	if len(msg.VHF) == 0 {
		log.Println("[download_debugtime] error: msg.VHF is empty!")
		resData = []byte(strconv.Itoa(200))
		return nil, fmt.Errorf("msg.VHF is empty!")
	}

	for k, v := range msg.VHF {
		if k >= 16 {
			break
		}
		indexKey.Hsh[k] = v
	}

	log.Println("[download_debugtime] B get vhf:", base58.Encode(msg.VHF))
	//res := message.DownloadShardResponse{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(config.Gconfig.DiskTimeout))
	defer cancel()

	log.Println("[download_debugtime] C  vhf:", base58.Encode(msg.VHF))
	time1 := time.Now()
	resData, err = dh.GetShard(ctx, indexKey)
	TokenPool.Dtp().DiskLatency.Add(time.Now().Sub(time1))
	if err != nil {
		log.Println("[download_debugtime] C0 Get data Slice fail:", base58.Encode(msg.VHF), pid.Pretty(), err)

		if msg.AllocId != "" {
			atomic.AddInt64(&statistics.DefaultStat.DownloadData404, 1)
		}
		return nil, fmt.Errorf("[download_debugtime] C1 Get data Slice fail:", base58.Encode(msg.VHF), pid.Pretty(), err)
	}
	log.Println("[download_debugtime] D Get data VHF=", base58.Encode(msg.VHF))
	if !msg.VerifyVHF(resData) {
		log.Println("[download_debugtime] D0 data verify failed: VHF=", base58.Encode(msg.VHF), "resData_Hash=", base58.Encode(message.CaculateHash(resData)))
		if msg.AllocId != "" {
			atomic.AddInt64(&statistics.DefaultStat.DownloadData404, 1)
		}
		return nil, fmt.Errorf("Get data Slice fail: slice VerifyVHF fail:", base58.Encode(msg.VHF), pid.Pretty())
	}

	log.Println("[download_debugtime] E data verify success: VHF=", base58.Encode(msg.VHF), "resData_Hash=", base58.Encode(message.CaculateHash(resData)))

	res.Data = resData
	resp, err := proto.Marshal(&res)
	if err != nil {
		log.Println("[download_debugtime] E0 Marshar response data fail:", err, "vhf:", base58.Encode(msg.VHF))
		if msg.AllocId != "" {
			atomic.AddInt64(&statistics.DefaultStat.DownloadData404, 1)
		}
		return nil, fmt.Errorf("Marshar response data fail:", err)
	}
	log.Println("[download_debugtime] F Get success vhf:", base58.Encode(msg.VHF))
	//atomic.AddInt64(&statistics.DefaultStat.TXSuccess, 1)
	//	log.Println("return msg", 0)
	return append(message.MsgIDDownloadShardResponse.Bytes(), resp...), err
}
func (dh *DownloadHandler) GetShard(ctx context.Context, key common.IndexTableKey) ([]byte, error) {
	shard := make(chan []byte)
	errC := make(chan error)

	go func() {
		buf, err := dh.YTFS().Get(key)
		if err != nil {
			errC <- err
			return
		}
		shard <- buf
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("read ytfs time out")
	case data := <-shard:
		return data, nil
	case err := <-errC:
		return nil, err
	}
}

// SpotCheckHandler 下载处理器
type SpotCheckHandler struct {
	StorageNode
}

func (sch *SpotCheckHandler) Handle(msgData []byte) []byte {
	var msg message.SpotCheckTaskList
	if err := proto.Unmarshal(msgData, &msg); err != nil {
		log.Println(err)
	}
	log.Println("收到抽查任务：", msg.TaskId, len(msg.TaskList), msg.TaskList)

	spotChecker := spotCheck.NewSpotChecker()
	spotChecker.TaskList = msg.TaskList
	spotChecker.TaskHandler = func(task *message.SpotCheckTask) bool {
		log.Printf("执行抽查任务%d [%s]\n", task.Id, task.Addr)
		if uint32(task.Id) == sch.Config().IndexID {
			return true
		}
		var checkres bool = false

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		clt, err := sch.Host().ClientStore().GetByAddrString(ctx, task.NodeId, []string{task.Addr})
		if err != nil {
			log.Println("连接失败", task.Id)
			return false
		}
		downloadRequest := &message.DownloadShardRequest{VHF: task.VHF}
		checkData, err := proto.Marshal(downloadRequest)
		if err != nil {
			log.Println("error:", err)
		}
		log.Printf("[抽查]下载分片消息 msg:%v buf len(%d)\n", task, len(checkData))
		// 发送下载分片命令
		if shardData, err := clt.SendMsg(ctx, message.MsgIDDownloadShardRequest.Value(), checkData); err != nil {
			log.Println("error:", err)
		} else {
			var share message.DownloadShardResponse
			if len(shardData) < 2 {
				log.Printf("抽查失败:返回消息为空")
				return false
			}
			if err := proto.Unmarshal(shardData[2:], &share); err != nil {
				log.Println("error:", err)
			} else {
				// 校验VHF
				checkres = downloadRequest.VerifyVHF(share.Data)
			}
		}
		log.Println("校验结果：", task.Id, checkres)
		return checkres
	}
	// 异步执行检查任务
	go func() {
		startTime := time.Now()
		spotChecker.Do()
		endTime := time.Now()
		log.Println("抽查任务结束用时:", endTime.Sub(startTime).String())
		if err := recover(); err != nil {
			log.Println("error:", err)
		}
		var replayMap = make(map[int][]int32)
		for _, v := range spotChecker.InvalidNodeList {
			row := replayMap[int(v)%len(sch.Config().BPList)]
			replayMap[int(v)%len(sch.Config().BPList)] = append(row, v)
		}
		for k, v := range replayMap {
			resp, err := proto.Marshal(&message.SpotCheckStatus{
				TaskId:          msg.TaskId,
				InvalidNodeList: v,
			})
			if err != nil {
				log.Println("error:", err)
			}
			log.Println("上报失败的任务：", v, "sn:", k)
			if r, e := sch.SendBPMsg(k, message.MsgIDSpotCheckStatus.Value(), resp); e != nil {
				log.Println("抽查任务上报失败：", e)
			} else {
				log.Printf("抽查任务上报成功%s\n", r)
			}
		}
	}()
	return append(message.MsgIDVoidResponse.Bytes())
}
