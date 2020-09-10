package recover

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/yottachain/YTDataNode/util"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/reedsolomon"
	"github.com/mr-tron/base58/base58"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	node "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTFS/common"
	lrcpkg "github.com/yottachain/YTLRC"
)

const (
	max_reply_num       = 1000
	max_task_num        = 1000
	max_reply_wait_time = time.Second * 60
)

type Task struct {
	SnID        int32
	Data        []byte
	ExpriedTime int64
}

type RecoverEngine struct {
	sn         node.StorageNode
	queue      chan *Task
	replyQueue chan *TaskMsgResult
	le         *LRCEngine
}

func New(sn node.StorageNode) (*RecoverEngine, error) {
	var re = new(RecoverEngine)
	re.queue = make(chan *Task, max_task_num)
	re.replyQueue = make(chan *TaskMsgResult, max_reply_num)
	re.sn = sn
	re.le = NewLRCEngine(re.getShard)

	return re, nil
}

func (re *RecoverEngine) Len() uint32 {
	return uint32(len(re.queue))
}

func (re *RecoverEngine) recoverShard(description *message.TaskDescription) error {
	defer func() {
		err := recover()
		fmt.Println("err:", err)
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
	var vhf [16]byte
	copy(vhf[:], description.Hashs[description.RecoverId])
	//err = re.sn.YTFS().Put(common.IndexTableKey(vhf), shards[int(description.RecoverId)])
	_, err = re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(vhf): shards[int(description.RecoverId)]})
	if err != nil && (err.Error() != "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value") {
		log.Printf("[recover:%s]YTFS Put error %s\n", base58.Encode(description.Id), err.Error())
		return err
	}
	return nil
}

func (re *RecoverEngine) getShard(ctx context.Context, id string, taskID string, addrs []string, hash []byte, n *int) ([]byte, error) {
	btid, err := base58.Decode(taskID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	clt, err := re.sn.Host().ClientStore().GetByAddrString(ctx, id, addrs)
	if err != nil {
		log.Printf("[recover:%d]get shard [%s] error[%d] %s addr %v id %d\n", BytesToInt64(btid[0:8]), base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
		return nil, err
	}

	// todo: 这里需要单独处理，连接自己失败的错误
	//if err != nil {
	//	if err.Error() == "new stream error:dial to self attempted" {
	//		var vhf [16]byte
	//		copy(vhf[:], hash)
	//		return re.sn.YTFS().Get(common.IndexTableKey(vhf))
	//	}
	//	return nil, err
	//}
	var getToken message.NodeCapacityRequest
	var resGetToken message.NodeCapacityResponse
	getToken.RequestMsgID = message.MsgIDDownloadShardRequest.Value()
	getTokenData, _ := proto.Marshal(&getToken)
	ctxto, cancels := context.WithTimeout(context.Background(), time.Second*5)
	defer cancels()
	tokenstart := time.Now()

RETRY:
	tok, err := clt.SendMsg(ctxto, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	proto.Unmarshal(tok[2:], &resGetToken)
	if err != nil  {
		log.Printf("[recover:%d]get shard [%s] get token error[%d] %s addr %v id %d\n", BytesToInt64(btid[0:8]), base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
		return nil, err
	}

	if !resGetToken.Writable {
		if time.Now().Sub(tokenstart).Seconds() > 10 {
			log.Println("[recover] get token err! resGetToken.AllocId=",resGetToken.AllocId)
			return nil,err
		}
		goto RETRY
	}

	var msg message.DownloadShardRequest
	var res message.DownloadShardResponse
	msg.VHF = hash
    msg.AllocId = resGetToken.AllocId

	buf, err := proto.Marshal(&msg)
	if err != nil {
		log.Printf("[recover:%d]get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		return nil, err
	}
	log.Printf("[recover]get shard msg buf len(%d)\n", len(buf))
	shardBuf, err := clt.SendMsgClose(ctx, message.MsgIDDownloadShardRequest.Value(), buf)

	if err != nil {
		log.Printf("[recover:%d]get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		return nil, err
	}
	err = proto.Unmarshal(shardBuf[2:], &res)
	if err != nil {
		log.Printf("[recover:%d]get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		return nil, err
	}

	log.Printf("[recover:%d]get shard [%s] success[%d]\n", BytesToInt64(btid[0:8]), base64.StdEncoding.EncodeToString(hash), *n)
	*n = *n + 1
	return res.Data, nil
}

type TaskMsgResult struct {
	ID          []byte
	RES         int32
	BPID        int32
	ExpriedTime int64
}

func (re *RecoverEngine) PutTask(task []byte, snid int32, expried int64) error {
	select {
	case re.queue <- &Task{
		SnID:        snid,
		Data:        task,
		ExpriedTime: expried,
	}:
	default:
	}
	return nil
}

// 多重建任务消息处理
func (re *RecoverEngine) HandleMuilteTaskMsg(msgData []byte) error {
	log.Printf("[recover]received multi recover task, pack size %d\n", len(msgData))
	var mtdMsg message.MultiTaskDescription
	if err := proto.Unmarshal(msgData, &mtdMsg); err != nil {
		return err
	}
	log.Printf("[recover]multi recover task start, pack size %d\n", len(mtdMsg.Tasklist))
	for _, task := range mtdMsg.Tasklist {
		bys := task[12:14]
		bytebuff := bytes.NewBuffer(bys)
		var snID uint16
		binary.Read(bytebuff, binary.BigEndian, &snID)
		//log.Printf("[recover]task bytes is %s, SN ID is %d\n", hex.EncodeToString(task[0:14]), int32(snID))

		// 如果大于过期时间跳过
		if time.Now().Unix() > mtdMsg.ExpiredTime {
			continue
		}
		if err := re.PutTask(task, int32(snID), mtdMsg.ExpiredTime); err != nil {
			log.Printf("[recover]put recover task error: %s\n", err.Error())
		}
	}
	return nil
}

func (re *RecoverEngine) Run() {
	go func() {
		for {
			ts := <-re.queue
			msg := ts.Data
			if bytes.Equal(msg[0:2], message.MsgIDTaskDescript.Bytes()) {
				res := re.execRCTask(msg[2:], ts.ExpriedTime)
				res.BPID = ts.SnID
				re.PutReplyQueue(res)
			} else if bytes.Equal(msg[0:2], message.MsgIDLRCTaskDescription.Bytes()) {
				log.Printf("[recover]LRC start\n")
				res := re.execLRCTask(msg[2:], ts.ExpriedTime)
				res.BPID = ts.SnID
				re.PutReplyQueue(res)
			} else {
				res := re.execCPTask(msg[2:], ts.ExpriedTime)
				res.BPID = ts.SnID
				re.PutReplyQueue(res)
			}
		}
	}()

	for {
		re.MultiReply()
	}
}

func (re *RecoverEngine) PutReplyQueue(res *TaskMsgResult) {
	select {
	case re.replyQueue <- res:
	default:
	}
}

func (re *RecoverEngine) reply(res *TaskMsgResult) error {
	var msgData message.TaskOpResult
	msgData.Id = res.ID
	msgData.RES = res.RES
	data, err := proto.Marshal(&msgData)
	if err != nil {
		return err
	}
	_, err = re.sn.SendBPMsg(int(res.BPID), message.MsgIDTaskOPResult.Value(), data)
	log.Println("[recover] reply to", int(res.BPID))
	return err
}

//
func (re *RecoverEngine) MultiReply() error {
	var resmsg = make(map[int32]*message.MultiTaskOpResult)

	func() {
		for i := 0; i < max_reply_num; i++ {
			select {
			case res := <-re.replyQueue:
				if resmsg[res.BPID] == nil {
					resmsg[res.BPID] = &message.MultiTaskOpResult{}
				}
				_r := resmsg[res.BPID]

				_r.Id = append(_r.Id, res.ID)
				_r.RES = append(_r.RES, res.RES)
				_r.ExpiredTime = res.ExpriedTime
				resmsg[res.BPID] = _r

			case <-time.After(max_reply_wait_time):
				return
			}
		}
	}()
	if l := len(resmsg); l > 0 {
		fmt.Println("待上报重建消息：", len(resmsg))
	}
	for k, v := range resmsg {
		v.NodeID = int32(re.sn.Config().IndexID)
		if data, err := proto.Marshal(v); err != nil {
			log.Printf("[recover]marsnal failed %s\n", err.Error())
			continue
		} else {
			re.sn.SendBPMsg(int(k), message.MsgIDMultiTaskOPResult.Value(), data)
			log.Printf("[recover] multi reply success nodeID %d, expried %d\n", v.NodeID, v.ExpiredTime)
		}
	}

	return nil
}

func (re *RecoverEngine) execRCTask(msgData []byte, expried int64) *TaskMsgResult {
	var res TaskMsgResult
	res.ExpriedTime = expried
	var msg message.TaskDescription
	if err := proto.Unmarshal(msgData, &msg); err != nil {
		log.Printf("[recover]proto解析错误%s", err)
		res.RES = 1
	}
	res.ID = msg.Id
	if err := re.recoverShard(&msg); err != nil {
		res.RES = 1
	} else {
		res.RES = 0
	}
	return &res
}

func (re *RecoverEngine) execLRCTask(msgData []byte, expried int64) *TaskMsgResult {

	var res TaskMsgResult
	res.ExpriedTime = expried
	var msg message.TaskDescription

	if err := proto.Unmarshal(msgData, &msg); err != nil {
		log.Printf("[recover]proto解析错误%s", err)
		res.RES = 1
	}

	res.ID = msg.Id
	res.RES = 1
	log.Printf("[recover]LRC 分片恢复开始%d", BytesToInt64(msg.Id[0:8]))
	defer log.Printf("[recover]LRC 分片恢复结束%d", BytesToInt64(msg.Id[0:8]))

	lrc := lrcpkg.Shardsinfo{}

	lrc.OriginalCount = uint16(len(msg.Hashs) - int(msg.ParityShardCount))
	log.Printf("[recover]LRC original count is %d", lrc.OriginalCount)
	lrc.RecoverNum = 13
	lrc.Lostindex = uint16(msg.RecoverId)
	h, err := re.le.GetLRCHandler(&lrc)
	if err != nil {
		log.Printf("[recover]LRC 获取Handler失败%s", err)
		return &res
	}
	log.Printf("[recover]lost idx %d: %s\n", lrc.Lostindex, base64.StdEncoding.EncodeToString(msg.Hashs[msg.RecoverId]))
	recoverData, err := h.Recover(msg)
	if err != nil {
		log.Printf("[recover]LRC 恢复失败%s", err)
		return &res
	}
	log.Printf("[recover]LRC 恢复的分片数据: %s", hex.EncodeToString(recoverData[0:128]))
	m5 := md5.New()
	m5.Write(recoverData)
	hash := m5.Sum(nil)
	// 校验hash失败
	if !bytes.Equal(hash, msg.Hashs[msg.RecoverId]) {
		log.Printf("[recover]LRC 校验HASH失败%s %s\n", base58.Encode(hash), base58.Encode(msg.Hashs[msg.RecoverId]))
		exec.Command("rm -rf recover*").Output()
		for k, v := range h.GetShards() {
			fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), fmt.Sprintf("recover-shard-%d", k)), os.O_CREATE|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				continue
			}
			fl.Write(v)
			fl.Close()
		}
		log.Printf("[recover]错误分片数据已保存 %s recoverID %x hash %s\n", BytesToInt64(msg.Id[0:8]), msg.RecoverId, base58.Encode(msg.Hashs[msg.RecoverId]))
		return &res
	}

	var key [common.HashLength]byte
	copy(key[:], hash)
	//if err := re.sn.YTFS().Put(common.IndexTableKey(key), recoverData); err != nil && err.Error() != "YTFS: hash key conflict happens" {
	if _, err := re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(key): recoverData}); err != nil && err.Error() != "YTFS: hash key conflict happens" {
		log.Printf("[recover]LRC 保存已恢复分片失败%s\n", err)
		return &res
	}

	log.Printf("[recover]LRC 分片恢复成功\n")
	res.RES = 0
	return &res
}

// 副本集任务
func (re *RecoverEngine) execCPTask(msgData []byte, expried int64) *TaskMsgResult {
	var msg message.TaskDescriptionCP
	var result TaskMsgResult
	result.ExpriedTime = expried
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
			var vhf [16]byte
			copy(vhf[:], msg.DataHash)
			// err := re.sn.YTFS().Put(common.IndexTableKey(vhf), shard)
			_, err := re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(vhf): shard})
			// 存储分片没有错误，或者分片已存在返回0，代表成功
			if err != nil && (err.Error() != "YTFS: hash key conflict happens" || err.Error() == "YTFS: conflict hash value") {
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

//BytesToInt64 convet byte slice to int64
func BytesToInt64(bys []byte) int64 {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return data
}
