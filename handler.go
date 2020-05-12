package node

import (
	"context"
	"fmt"
	"github.com/yottachain/YTDataNode/statistics"
	"log"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/spotCheck"
	"github.com/yottachain/YTDataNode/uploadTaskPool"

	"github.com/yottachain/YTDataNode/message"

	"github.com/golang/protobuf/proto"
	. "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTFS/common"
)

// WriteHandler 写入处理器
type WriteHandler struct {
	StorageNode
	Upt          *uploadTaskPool.UploadTaskPool
	RequestQueue chan *wRequest
}

func NewWriteHandler(sn StorageNode, utp *uploadTaskPool.UploadTaskPool) *WriteHandler {
	return &WriteHandler{
		sn,
		utp,
		make(chan *wRequest, 30000),
	}
}

type wRequest struct {
	Key   common.IndexTableKey
	Data  []byte
	Error chan error
}

func (wh *WriteHandler) push(ctx context.Context, key common.IndexTableKey, data []byte) error {
	rq := &wRequest{
		key,
		data,
		make(chan error),
	}
	select {
	case wh.RequestQueue <- rq:
		log.Println("[task]push task success")
	default:
		return fmt.Errorf("task busy", len(wh.RequestQueue))
	}
	select {
	case err := <-rq.Error:
		return err
	case <-ctx.Done():
		return fmt.Errorf("get error time out")
	}
}
func (wh *WriteHandler) batchWrite(number int) {
	rqmap := make(map[common.IndexTableKey][]byte, number)
	rqs := make([]*wRequest, number)
	for i := 0; i < number; i++ {
		select {
		case rq := <-wh.RequestQueue:
			rqmap[rq.Key] = rq.Data
			rqs[i] = rq
		default:
			continue
		}
	}

	log.Printf("[ytfs]flush start:%d\n", number)
	_, err := wh.YTFS().BatchPut(rqmap)
	if err == nil {
		log.Printf("[ytfs]flush sucess:%d\n", number)
	} else {
		log.Printf("[ytfs]flush failure:%s\n", err.Error())
	}

	for _, rq := range rqs {
		select {
		case rq.Error <- err:
		default:
			continue
		}
	}
}

func (wh *WriteHandler) Run() {
	go wh.Upt.FillToken(context.Background())
	go func() {
		var flushInterval time.Duration = time.Millisecond * 100
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

func (wh *WriteHandler) GetToken(data []byte, id peer.ID) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	tk, err := wh.Upt.Get(ctx, id)

	// 如果 剩余空间不足10个分片停止发放token
	if t, u := wh.YTFS().DiskAndUseCap(); u+wh.YTFS().Meta().DataBlockSize*10 >= t {
		tk = nil
		err = fmt.Errorf("YTFS： space is not enough", t, u)
	}

	var res message.NodeCapacityResponse
	res.Writable = true
	if err != nil {
		res.Writable = false
		log.Println(err)
	} else {
		res.AllocId = tk.String()
	}
	// 如果token为空 返回 假
	if res.AllocId == "" {
		res.Writable = false
	}
	resbuf, _ := proto.Marshal(&res)
	log.Printf("[task pool]get token return [%s]\n", tk.String())

	return append(message.MsgIDNodeCapacityResponse.Bytes(), resbuf...)
}

// Handle 获取回调处理函数
func (wh *WriteHandler) Handle(msgData []byte) []byte {
	startTime := time.Now()
	var msg message.UploadShardRequest
	proto.Unmarshal(msgData, &msg)

	log.Printf("shard [VHF:%s] need save \n", base58.Encode(msg.VHF))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
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
	// 计数
	statistics.DefaultStat.AddSaveRequestCount()

	log.Printf("[task pool][%s]check allocID[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
	if msg.AllocId == "" {
		// buys
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		return 105
	}
	tk, err := uploadTaskPool.NewTokenFromString(msg.AllocId)
	if err != nil {
		// buys
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		log.Println("token check error：", err.Error())
		return 105
	}
	if !wh.Upt.Check(tk) {
		log.Printf("[task pool][%s]task bus[%s]\n", base58.Encode(msg.VHF), msg.AllocId)
		log.Println("token check fail：", tk.String())
		return 105
	}
	defer wh.Upt.Delete(tk)
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
	err = wh.push(ctx, common.IndexTableKey(indexKey), msg.DAT)
	if err != nil {
		log.Println(fmt.Errorf("Write data slice fail:%s", err))
		if err.Error() == "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value" {
			return 102
		}
		log.Println("数据写入错误error:", err)
		return 101
	}
	log.Println("return msg", 0)

	// 成功计数
	statistics.DefaultStat.AddSaveSuccessCount()
	return 0
}

// DownloadHandler 下载处理器
type DownloadHandler struct {
	StorageNode
}

// Handle 获取处理器
func (dh *DownloadHandler) Handle(msgData []byte, pid peer.ID) []byte {
	var msg message.DownloadShardRequest
	var indexKey [16]byte
	proto.Unmarshal(msgData, &msg)
	log.Println("get vhf:", base58.Encode(msg.VHF))

	for k, v := range msg.VHF {
		if k >= 16 {
			break
		}
		indexKey[k] = v
	}
	res := message.DownloadShardResponse{}
	resData, err := dh.YTFS().Get(common.IndexTableKey(indexKey))
	if msg.VerifyVHF(resData) {
		log.Println("data verify success")
	}
	if err != nil {
		log.Println("Get data Slice fail:", base58.Encode(msg.VHF), pid.Pretty(), err)
	}
	res.Data = resData
	resp, err := proto.Marshal(&res)
	if err != nil {
		log.Println("Marshar response data fail:", err)
	}
	log.Println("return msg", 0)
	return append(message.MsgIDDownloadShardResponse.Bytes(), resp...)
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
		// 发送下载分片命令
		if shardData, err := clt.SendMsgClose(ctx, message.MsgIDDownloadShardRequest.Value(), checkData); err != nil {
			log.Println("error:", err)
		} else {
			var share message.DownloadShardResponse
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
