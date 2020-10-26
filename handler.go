package node

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/TaskPool"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/statistics"
	yhservice "github.com/yottachain/YTHost/service"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/yottachain/YTDataNode/logger"
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
	db           *leveldb.DB
}

func NewWriteHandler(sn StorageNode) *WriteHandler {
	return &WriteHandler{
		sn,
		make(chan *wRequest, 1000),
		nil,
	}
}

type wRequest struct {
	Key   common.IndexTableKey
	Data  []byte
	Error chan error
}

func (wh *WriteHandler) Put(ctx context.Context, key common.IndexTableKey, data []byte) error {
	req := make(map[common.IndexTableKey][]byte, 1)
	req[key] = data
	_, err := wh.YTFS().BatchPut(req)
	return err
}

func (wh *WriteHandler) Run() {
	go TaskPool.Utp().FillToken()
	go TaskPool.Dtp().FillToken()
}

func (wh *WriteHandler) GetToken(data []byte, id peer.ID) []byte {
	atomic.AddInt64(&statistics.DefaultStat.RequestToken, 1)
	var GTMsg message.NodeCapacityRequest
	var xtp *TaskPool.TaskPool = TaskPool.Utp()
	var isUpload = true
	err := proto.Unmarshal(data, &GTMsg)
	if err == nil && GTMsg.RequestMsgID == message.MsgIDDownloadShardRequest.Value() || GTMsg.RequestMsgID == message.MsgIDMultiTaskDescription.Value() {
		xtp = TaskPool.Dtp()
		isUpload = false
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Gconfig.TokenWait)*time.Millisecond)
	if config.Gconfig.TokenWait == 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	}
	defer cancel()

	tk, err := xtp.Get(ctx, id, 0)
	if err != nil {
		fmt.Println("[get token] error:", err.Error())
	}

	// 如果 剩余空间不足10个分片停止发放token
	if disableWrite || wh.YTFS().Meta().YtfsSize/uint64(wh.YTFS().Meta().DataBlockSize) <= (wh.YTFS().Len()+10) {
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
		if isUpload {
			atomic.AddInt64(&statistics.DefaultStat.SentTokenNum, 1)
		} else {
			atomic.AddInt64(&statistics.DefaultStat.SentDownloadTokenNum, 1)
		}
	}
	resbuf, _ := proto.Marshal(&res)
	if tk != nil {
		log.Printf("[task pool]get token return [%s]\n", tk.String())
	}

	return append(message.MsgIDNodeCapacityResponse.Bytes(), resbuf...)
}

// Handle 获取回调处理函数
func (wh *WriteHandler) Handle(msgData []byte, head yhservice.Head) []byte {
	statistics.DefaultStat.Lock()
	statistics.DefaultStat.SaveRequestCount++
	statistics.DefaultStat.Unlock()

	startTime := time.Now()
	var msg message.UploadShardRequest
	proto.Unmarshal(msgData, &msg)

	log.Printf("shard [VHF:%s] need save \n", base58.Encode(msg.VHF))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	ctx = context.WithValue(ctx, "pid", head.RemotePeerID)
	defer cancel()
	// 添加超时
	resCode := wh.saveSlice(ctx, msg)
	if resCode != 0 {
		log.Printf("shard [VHF:%s] write failed [%f]\n", base58.Encode(msg.VHF), time.Now().Sub(startTime).Seconds())
	} else {
		log.Printf("shard [VHF:%s] write success [%f]\n", base58.Encode(msg.VHF), time.Now().Sub(startTime).Seconds())
	}

	res2client, err := msg.GetResponseToClientByCode(resCode, wh.Config().PrivKeyString())
	if err != nil {
		log.Println("Get res code 2 client fail:", err)
		log.Printf("shard [VHF:%s] return client failed [%f]\n", base58.Encode(msg.VHF), time.Now().Sub(startTime).Seconds())
	}
	return res2client
}

func (wh *WriteHandler) saveSlice(ctx context.Context, msg message.UploadShardRequest) int32 {

	log.Printf("[task pool][%s]check allocID[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
	if msg.AllocId == "" {
		// buys
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		return 105
	}
	tk, err := TaskPool.NewTokenFromString(msg.AllocId)
	if err != nil {
		// buys
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		log.Println("token check error：", err.Error())
		pid := ctx.Value("pid").(peer.ID)

		wh.Host().ClientStore().Close(pid)
		recover()
		return 105
	}
	if !TaskPool.Utp().Check(tk) {
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		log.Println("token check fail：", time.Now().Sub(tk.Tm).Milliseconds())
		return 105
	}
	TaskPool.Utp().NetLatency.Add(time.Now().Sub(tk.Tm))

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
		return 100
	}
	// 3. 将数据写入YTFS-disk
	var indexKey [16]byte
	copy(indexKey[:], msg.VHF[0:16])
	putStartTime := time.Now()
	err = wh.Put(ctx, common.IndexTableKey(indexKey), msg.DAT)
	//err = wh.YTFS().Put(common.IndexTableKey(indexKey), msg.DAT)
	if err != nil {
		if err.Error() == "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value" {
			return 102
		}
		log.Println("数据写入错误error:", err.Error())
		return 101
	}
	log.Println("return msg", 0)

	diskltc := time.Now().Sub(putStartTime)
	// 成功计数
	TaskPool.Utp().DiskLatency.Add(diskltc)
	if diskltc > time.Second*10 {
		log.Printf("[disklatency] %f s\n", diskltc.Seconds())
	}

	defer TaskPool.Utp().Delete(tk)
	return 0
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

	var indexKey [16]byte
	err = proto.Unmarshal(msgData, &msg)
	if err != nil {
		fmt.Println("Unmarshal error:", err)
		return nil, err
	}

	if msg.AllocId != "" {
		if tk, err := TaskPool.NewTokenFromString(msg.AllocId); err == nil {
			defer TaskPool.Dtp().Delete(tk)
		}
	}

	log.Println("get vhf:", base58.Encode(msg.VHF))
	if len(msg.VHF) == 0 {
		log.Println("error: msg.VHF is empty!")
		resData = []byte(strconv.Itoa(200))
		return nil, fmt.Errorf("msg.VHF is empty!")
	}

	for k, v := range msg.VHF {
		if k >= 16 {
			break
		}
		indexKey[k] = v
	}

	//res := message.DownloadShardResponse{}
	resData, err = dh.YTFS().Get(common.IndexTableKey(indexKey))
	if err != nil {
		log.Println("Get data Slice fail:", base58.Encode(msg.VHF), pid.Pretty(), err)
		//		resData = []byte(strconv.Itoa(201))
		return nil, fmt.Errorf("Get data Slice fail:", base58.Encode(msg.VHF), pid.Pretty(), err)
	}

	if !msg.VerifyVHF(resData) {
		log.Println("data verify failed: VHF=",base58.Encode(msg.VHF),"resData_Hash=",base58.Encode(message.CaculateHash(resData)))
		return nil,fmt.Errorf("Get data Slice fail: slice VerifyVHF fail:", base58.Encode(msg.VHF), pid.Pretty())
	}

	log.Println("data verify success: VHF=",base58.Encode(msg.VHF),"resData_Hash=",base58.Encode(message.CaculateHash(resData)))

	res.Data = resData
	resp, err := proto.Marshal(&res)
	if err != nil {
		log.Println("Marshar response data fail:", err)
		return nil, fmt.Errorf("Marshar response data fail:", err)
	}
	//	log.Println("return msg", 0)
	return append(message.MsgIDDownloadShardResponse.Bytes(), resp...), err
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
	log.Println()
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
		if shardData, err := clt.SendMsgClose(ctx, message.MsgIDDownloadShardRequest.Value(), checkData); err != nil {
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
