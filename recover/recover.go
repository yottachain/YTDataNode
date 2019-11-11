package recover

import (
	"bytes"
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/kkdai/diskqueue"
	"github.com/klauspost/reedsolomon"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/host"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/common"
	"path"
	"sync"
	"time"
)

type RecoverEngine struct {
	host  *host.Host
	ytfs  *ytfs.YTFS
	queue diskqueue.WorkQueue
}

func New(hst *host.Host, yt *ytfs.YTFS) (*RecoverEngine, error) {
	var re = new(RecoverEngine)
	re.host = hst
	re.ytfs = yt
	re.queue = diskqueue.NewDiskqueue("taskQueue", util.GetYTFSPath())
	return re, nil
}

func (re *RecoverEngine) recoverShard(description *message.TaskDescription) error {
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
	err = re.ytfs.Put(common.IndexTableKey(vhf), shards[int(description.RecoverId)])
	if err != nil && err.Error() != "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value" {
		log.Printf("[recover:%s]YTFS Put error %s\n", base58.Encode(description.Id), err.Error())
		return err
	}
	return nil
}

func (re *RecoverEngine) getShard(ctx context.Context, id string, taskID string, addrs []string, hash []byte, n *int) ([]byte, error) {
	err := re.host.ConnectAddrStrings(id, addrs)
	if err != nil {
		return nil, err
	}
	stm, err := re.host.NewMsgStream(ctx, id, "/node/0.0.2")
	if err != nil {
		if err.Error() == "new stream error:dial to self attempted" {
			var vhf [32]byte
			copy(vhf[:], hash)
			return re.ytfs.Get(common.IndexTableKey(vhf))
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

type RCTaskMsgResult struct {
	ID  []byte
	RES int32
}

func (re *RecoverEngine) HandleMsg(msgData []byte, stm *host.MsgStream) error {
	var res message.TaskOpResult
	r := re.execRCTask(msgData)
	res.Id = r.ID
	res.RES = r.RES
	buf, err := proto.Marshal(&res)
	if err != nil {
		return err
	}
	if err := re.replay(message.MsgIDTaskOPResult.Bytes(), buf, stm); err != nil {
		return err
	}
	log.Printf("[recover:%s]success\n", base58.Encode(res.Id))
	return nil
}

func (re *RecoverEngine) PutTask(task []byte) error {
	if err := re.queue.Put(task); err != nil {
		return err
	}
	return nil
}

func (re *RecoverEngine) execRCTask(msgData []byte) *RCTaskMsgResult {
	var res RCTaskMsgResult
	var msg message.TaskDescription
	if err := proto.Unmarshal(msgData, &msg); err != nil {
		log.Printf("[recover]proto解析错误%s", err)
		res.RES = 0
	}
	res.ID = msg.Id
	if err := re.recoverShard(&msg); err != nil {
		res.RES = 1
	} else {
		res.RES = 0
	}
	return &res
}

// 多重建任务消息处理
func (re *RecoverEngine) HandleMuilteTaskMsg(msgData []byte, stm *host.MsgStream) error {
	// 结果集最大长度
	const maxResultLen = 10

	var mtdMsg message.MultiTaskDescription
	var multiTaskOPResults message.MultiTaskOpResult
	if err := proto.Unmarshal(msgData, &mtdMsg); err != nil {
		return err
	}
	log.Printf("[recover]multi recover task start, pack size %d\n", len(mtdMsg.Tasklist))
	defer log.Printf("[recover]multi recover task complite, pack size %d\n", len(mtdMsg.Tasklist))
	for index, task := range mtdMsg.Tasklist {
		func(msg []byte) {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("[recover]exec error:%s\n", err.(error).Error())
				}
			}()
			var r *RCTaskMsgResult
			if bytes.Equal(msg[0:2], message.MsgIDTaskDescript.Bytes()) {
				r = re.execRCTask(msg[2:])
			} else {
				r = re.execCPTask(msg[2:])
			}
			multiTaskOPResults.Id = append(multiTaskOPResults.Id, r.ID)
			multiTaskOPResults.RES = append(multiTaskOPResults.RES, r.RES)

			// 如果达到提交条件，满个或者已经遍历到数组尾部。提交结果,并清空结果
			if len(multiTaskOPResults.Id) >= maxResultLen || index >= len(mtdMsg.Tasklist)-1 {
				re.multiReply(&multiTaskOPResults, stm)
				multiTaskOPResults.Id = make([][]byte, 0)
				multiTaskOPResults.RES = make([]int32, 0)
			}
		}(task)
	}
	return nil
}

// 副本集任务
func (re *RecoverEngine) execCPTask(msgData []byte) *RCTaskMsgResult {
	var msg message.TaskDescriptionCP
	var result RCTaskMsgResult
	err := proto.UnmarshalMerge(msgData, &msg)
	if err != nil {
		log.Printf("[recover]解析错误%s\n", err.Error())
	}
	result.ID = msg.Id
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
			err := re.ytfs.Put(common.IndexTableKey(vhf), shard)
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

func (re *RecoverEngine) multiReply(multiTaskOPResults *message.MultiTaskOpResult, stm *host.MsgStream) {

	defer log.Printf("[recover] result %s, %v\n", util.IDS2String(multiTaskOPResults.Id), multiTaskOPResults.RES)

	buf, err := proto.Marshal(multiTaskOPResults)
	if err != nil {
		log.Println("recover", err)
	}
	if err := re.replay(message.MsgIDMultiTaskOPResult.Bytes(), buf, stm); err != nil {
		log.Println("recover", err)
	}
}

func (re *RecoverEngine) replay(msgid []byte, data []byte, stm *host.MsgStream) error {
	log.Printf("[recover:%s]reply to [%s]\n", stm.Conn().RemoteMultiaddr().String())
	defer log.Printf("[recover:%s]reply complite\n", stm.Conn().RemoteMultiaddr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := re.host.ConnectAddrStrings(stm.Conn().RemotePeer().Pretty(), []string{stm.Conn().RemoteMultiaddr().String()}); err != nil {
		return nil
	}
	stm, err := re.host.NewMsgStream(ctx, stm.Conn().RemotePeer().Pretty(), "/node/0.0.2")
	if err != nil {
		log.Printf("[recover:%s]reply error [%s]\n", stm.Conn().RemoteMultiaddr().String(), err.Error())
		return err
	}
	stm.SendMsg(append(msgid, data...))
	defer stm.Close()
	return nil
}
