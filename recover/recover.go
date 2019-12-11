package recover

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/kkdai/diskqueue"
	"github.com/klauspost/reedsolomon"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/host"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	node "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTFS/common"
	_ "net/http/pprof"
	"sync"
	"time"
)

type RecoverEngine struct {
	sn         node.StorageNode
	queue      diskqueue.WorkQueue
	replyQueue diskqueue.WorkQueue
}

func New(sn node.StorageNode) (*RecoverEngine, error) {
	var re = new(RecoverEngine)
	re.queue = diskqueue.NewDiskqueue(".taskQueue", util.GetYTFSPath())
	re.replyQueue = diskqueue.NewDiskqueue(".replyQueue", util.GetYTFSPath())
	re.sn = sn
	return re, nil
}

func (re *RecoverEngine) recoverShard(description *message.TaskDescription) error {
	defer func() {
		err := recover()
		fmt.Println(err)
	}()
	var size = len(description.Hashs)
	var shards [][]byte = make([][]byte, size)
	encoder, err := reedsolomon.New(size-int(description.ParityShardCount), int(description.ParityShardCount))
	if err != nil {
		return err
	}
	var wg = sync.WaitGroup{}
	var number int
	wg.Add(len(description.Locations))
	log.Printf("[recover:%s]recover start %d\n", base58.Encode(description.Id), size)
	for k, v := range description.Locations {
		go func(k int, v *message.P2PLocation) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			shard, err := re.getShard(ctx, v.NodeId, base58.Encode(description.Id), v.Addrs, description.Hashs[k], &number)
			if err == nil {
				shards[k] = shard
			} else {
				log.Printf("[recover:%s]error:%s, %v, %s\n", base58.Encode(description.Id), err.Error(), v.Addrs, v.NodeId)
			}
		}(k, v)
	}
	wg.Wait()
	shards[description.RecoverId] = nil
	err = encoder.Reconstruct(shards)
	if err != nil {
		log.Printf("[recover:%s]datas recover error:%s\n", base58.Encode(description.Id), err.Error())
		return err
	}
	log.Printf("[recover:%s]datas recover success\n", base58.Encode(description.Id))
	var vhf [32]byte
	copy(vhf[:], description.Hashs[description.RecoverId])
	err = re.sn.YTFS().Put(common.IndexTableKey(vhf), shards[int(description.RecoverId)])
	if err != nil && err.Error() != "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value" {
		log.Printf("[recover:%s]YTFS Put error %s\n", base58.Encode(description.Id), err.Error())
		return err
	}
	return nil
}

func (re *RecoverEngine) getShard(ctx context.Context, id string, taskID string, addrs []string, hash []byte, n *int) ([]byte, error) {

	err := re.sn.Host().ConnectAddrStrings(id, addrs)
	if err != nil {
		return nil, err
	}
	stm, err := re.sn.Host().NewMsgStream(ctx, id, "/node/0.0.2")
	if err != nil {
		if err.Error() == "new stream error:dial to self attempted" {
			var vhf [32]byte
			copy(vhf[:], hash)
			return re.sn.YTFS().Get(common.IndexTableKey(vhf))
		}
		return nil, err
	}

	var msg message.DownloadShardRequest
	var res message.DownloadShardResponse
	msg.VHF = hash
	buf, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}
	shardBuf, err := stm.SendMsgGetResponse(append(message.MsgIDDownloadShardRequest.Bytes(), buf...))

	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(shardBuf[2:], &res)
	if err != nil {
		return nil, err
	}
	*n = *n + 1
	log.Printf("[recover:%s]get shard [%s] success[%d]\n", taskID, base58.Encode(hash), *n)
	return res.Data, nil
}

type TaskMsgResult struct {
	ID   []byte
	RES  int32
	BPID byte
}

func (re *RecoverEngine) HandleMsg(msgData []byte, stm *host.MsgStream) error {
	return re.PutTask(append(message.MsgIDTaskDescript.Bytes(), msgData...))
}

func (re *RecoverEngine) PutTask(task []byte) error {
	go re.queue.Put(task)
	return nil
}

// 多重建任务消息处理
func (re *RecoverEngine) HandleMuilteTaskMsg(msgData []byte, stm *host.MsgStream) error {
	var mtdMsg message.MultiTaskDescription
	if err := proto.Unmarshal(msgData, &mtdMsg); err != nil {
		return err
	}
	log.Printf("[recover]multi recover task start, pack size %d\n", len(mtdMsg.Tasklist))
	for _, task := range mtdMsg.Tasklist {
		if err := re.PutTask(task); err != nil {
			log.Printf("[recover]put recover task error: %s\n", err.Error())
		}
	}
	return nil
}

func (re *RecoverEngine) Run() {
	for {
		msg := <-re.queue.ReadChan()
		if bytes.Equal(msg[0:2], message.MsgIDTaskDescript.Bytes()) {
			res := re.execRCTask(msg[2:])
			re.reply(res)
		} else {
			res := re.execCPTask(msg[2:])
			re.reply(res)
		}
	}
}

func (re *RecoverEngine) PutReplyQueue(res *TaskMsgResult) {
	//re.replyQueue.Put(res)
}

func (re *RecoverEngine) reply(res *TaskMsgResult) error {
	var msgData message.TaskOpResult
	msgData.Id = res.ID
	msgData.RES = res.RES
	data, err := proto.Marshal(&msgData)
	if err != nil {
		return err
	}
	_, err = re.sn.SendBPMsg(int(res.BPID), append(message.MsgIDTaskOPResult.Bytes(), data...))
	log.Println("[recover] reply to", int(res.BPID))
	return err
}

//func (re *RecoverEngine) MultiReply()error {
//	var resmsg message.MultiTaskOpResult
//	for i := 0; i < 10; i++ {
//		select {
//		case res := <-re.replyQueue:
//			resmsg.Id = append(resmsg.Id, res.ID)
//			resmsg.RES = append(resmsg.RES, res.RES)
//		case <-time.After(10 * time.Second):
//			break
//		}
//	}
//	data, err := proto.Marshal(&resmsg)
//	if err != nil {
//		return err
//	}
//	_, err = re.sn.SendBPMsg(int(res.BPID), append(message.MsgIDTaskOPResult.Bytes(), data...))
//	return err
//}

func (re *RecoverEngine) execRCTask(msgData []byte) *TaskMsgResult {
	var res TaskMsgResult
	var msg message.TaskDescription
	if err := proto.Unmarshal(msgData, &msg); err != nil {
		log.Printf("[recover]proto解析错误%s", err)
		res.RES = 0
	}
	res.ID = msg.Id
	res.BPID = msg.Id[12]
	if err := re.recoverShard(&msg); err != nil {
		res.RES = 1
	} else {
		res.RES = 0
	}
	return &res
}

// 副本集任务
func (re *RecoverEngine) execCPTask(msgData []byte) *TaskMsgResult {
	var msg message.TaskDescriptionCP
	var result TaskMsgResult
	err := proto.UnmarshalMerge(msgData, &msg)
	if err != nil {
		log.Printf("[recover]解析错误%s\n", err.Error())
	}
	result.ID = msg.Id
	result.BPID = msg.Id[12]
	result.RES = 1
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var number int
	// 循环从副本节点获取分片，只要有一个成功就返回
	for _, v := range msg.Locations {
		shard, err := re.getShard(ctx, v.NodeId, base58.Encode(msg.Id), v.Addrs, msg.DataHash, &number)
		// 如果没有发生错误，分片下载成功，就存储分片
		if err == nil {
			var vhf [32]byte
			copy(vhf[:], msg.DataHash)
			err := re.sn.YTFS().Put(common.IndexTableKey(vhf), shard)
			// 存储分片没有错误，或者分片已存在返回0，代表成功
			if err != nil && err.Error() != "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value" {
				log.Printf("[recover:%s]YTFS Put error %s\n", base58.Encode(vhf[:]), err.Error())
				result.RES = 1
			} else {
				result.RES = 0
			}
			break
		}
	}
	return &result
}
