package recover

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	//"github.com/docker/docker/pkg/locker"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/reedsolomon"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/TaskPool"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	node "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTFS/common"
	lrcpkg "github.com/yottachain/YTLRC"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"
	"strings"
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
	sn           node.StorageNode
	queue        chan *Task
	replyQueue   chan *TaskMsgResult
	le           *LRCEngine
	tstdata      [164]lrcpkg.Shard
	rebuildTask     uint64
	concurrentTask  uint64
	concurrenGetShard  uint64
	successRebuild  uint64
	failRebuild     uint64
	reportTask      uint64
	getShardWkCnt   uint64
	failDecodeTaskID uint64
	successShard    uint64
	failShard       uint64
	failSendShard   uint64
	failToken       uint64
	failConn        uint64
	failLessShard   uint64
	passJudge       uint64
	sucessConn      uint64
	successToken    uint64
	Upt             *TaskPool.TaskPool
}

func New(sn node.StorageNode) (*RecoverEngine, error) {
	var re = new(RecoverEngine)
	re.queue = make(chan *Task, max_task_num)
	re.replyQueue = make(chan *TaskMsgResult, max_reply_num)
	re.sn = sn
	re.le = NewLRCEngine(re.getShard)
    re.Upt = TaskPool.Utp()
	return re, nil
}

func (re *RecoverEngine) Len() uint32 {
	return uint32(len(re.queue))
}

type RecoverStat struct {
	RebuildTask     uint64 `json:"RebuildTask"`                         //下发重建的任务总数
	ConcurrentTask  uint64  `json:"ConcurrentTask"`                     //并发进行的重建任务数
	ConcurrenGetShard  uint64  `json:"ConcurenGetShard"`                //并发拉取分片数
	SuccessRebuild  uint64 `json:"SuccessRebuild"`                      //成功的重建的任务总数
	FailRebuild  uint64  `json:"FailRebuild"`                           //重建失败的任务总数
	ReportTask   uint64   `json:"ReportTask"`                           //上报的任务总数（包括重建成功和失败的上报）
	GetShardWkCnt   uint64 `json:"getShardWkCnt"`                       //拉取分片的总次数
	FailDecodeTaskID uint64  `json:"failDecodeTaskID"`                  //拉取分片时，解码需拉取的分片信息的错误总数（有可能需拉取的分片信息不全）
	Success   uint64 `json:"Success"`                                   //成功拉取的分片总数
	FailShard uint64 `json:"FailShard"`                                 //不存在的总分片数（分片丢失）
	FailSendShard  uint64 `json:"FailSendShard"`                        //分片存在，但是传输过程中分片丢失
	FailToken uint64 `json:"FailToken"`                                 //拉取分片时，获取不到token的总次数
	FailConn  uint64 `json:"failConn"`                                  //拉取分片时，无法连接的总数
	FailLessShard   uint64 `json:"failLessShard"`                       //在线矿机数不够，无法获取足够分片
	PassJudge       uint64 `json:"passJudge"`                           //预判重建成功
	SuccessConn      uint64 `json:"sucessConn"`                          //连接成功数
	SuccessToken    uint64 `json:"successToken"`                        //获取token成功
}

//RebuildTask = ReportTask    （近似相等）
//RebuildTask = SuccessRebuild + FailRebuild  （近似相等）
//GetShardWkCnt = FailDecodeTaskID + Success + FailShard + FailSendShard + FailToken + FailConn （近似相等）
//ConcurrentTask = ConcurrenGetShard （理想情况）

func (re *RecoverEngine) GetStat() *RecoverStat {
	return &RecoverStat{
		re.rebuildTask,
		re.concurrentTask,
		re.concurrenGetShard,
		re.successRebuild,
		re.failRebuild,
		re.getShardWkCnt,
		re.reportTask,
		re.failDecodeTaskID,
		re.successShard,
		re.failShard,
		re.failSendShard,
		re.failToken,
		re.failConn,
		re.failLessShard,
		re.passJudge,
		re.sucessConn,
		re.successToken,
	}
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
			shard, err := re.getShard2(ctx, v.NodeId, base58.Encode(description.Id), v.Addrs, description.Hashs[k], &number)
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

func (re *RecoverEngine) getShard2(ctx context.Context, id string, taskID string, addrs []string, hash []byte, n *int) ([]byte, error) {
	return nil,nil    //refer to getShard
}

func (re *RecoverEngine) getShard( id string, taskID string, addrs []string, hash []byte, n *int,sw *Switchcnt) ([]byte, error) {
	re.IncGetShardWK()
	btid, err := base58.Decode(taskID)
	if err != nil {
		re.IncFailDcdTask()
		return nil, err
	}

	if 0 == len(id) {
		err = fmt.Errorf("zero length id")
		re.IncFailDcdTask()
		return nil, err
	}

	if 0 == len(addrs) {
		err = fmt.Errorf("zero length addrs")
		re.IncFailDcdTask()
		return nil, err
	}

	if 0 == len(hash) {
		err = fmt.Errorf("zero length hash")
		re.IncFailDcdTask()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	connStart := time.Now()

CONNRTY:
	clt, err := re.sn.Host().ClientStore().GetByAddrString(ctx, id, addrs)
	if err != nil {
		if time.Now().Sub(connStart).Seconds() >3{
			re.IncFailConn()
			log.Printf("[recover:%d] failConn[%v] get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.failConn, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
			return nil, err
		}
		goto CONNRTY
	}

	if 0 == sw.swconn {
		re.IncSuccConn()
		sw.swconn++
	}


	var getToken message.NodeCapacityRequest
	var resGetToken message.NodeCapacityResponse
	getToken.RequestMsgID = message.MsgIDMultiTaskDescription.Value()
	getTokenData, _ := proto.Marshal(&getToken)

	re.GetConTaskPass()
	ctxto, cancels := context.WithTimeout(context.Background(), time.Second*15)
	defer cancels()

	//localctx2, localcancel2 := context.WithTimeout(context.Background(), time.Second*14)
	//defer localcancel2()
	//var localTokenW *TaskPool.Token
	// wtokenstart := time.Now()
	//for {
	//	localTokenW, err = re.Upt.Get(localctx2, peer.ID("11111111111"), 3)
	//	if err == nil {
	//		break
	//	}
	//	if time.Now().Sub(wtokenstart).Seconds() > 3 {
	//		err = fmt.Errorf("faild to get localTokenW")
	//		log.Printf("[recover] get localTokenW outtime!")
	//		return nil,err
	//	}
	//}

	tokenstart := time.Now()

RETRY:
	//tok, err := clt.SendMsg(ctxto, message.MsgIDMultiTaskDescription.Value(), getTokenData)
	tok, err := clt.SendMsg(ctxto, message.MsgIDNodeCapacityRequest.Value(), getTokenData)

	if err != nil || len(tok) < 3 {
		if time.Now().Sub(tokenstart).Seconds() > 10 {
			re.IncFailToken()
			err = fmt.Errorf("faild to get token")
			log.Printf("[recover] failToken [%v] get token err! resGetToken.AllocId=%v", re.failToken, resGetToken.AllocId)
			//re.Upt.Delete(localTokenW)
			re.ReturnConTaskPass()
			return nil,err
		}
		goto RETRY
	}

	err = proto.Unmarshal(tok[2:], &resGetToken)
	if err != nil {
		if time.Now().Sub(tokenstart).Seconds() > 5 {
			re.IncFailToken()
			log.Printf("[recover] failToken [%v] get token err! resGetToken.AllocId=%v", re.failToken, resGetToken.AllocId)
			//re.Upt.Delete(localTokenW)
			re.ReturnConTaskPass()
			return nil,err
		}
		goto RETRY
	}

	if !resGetToken.Writable {
		if time.Now().Sub(tokenstart).Seconds() > 5 {
			re.IncFailToken()
			err = fmt.Errorf("resGetToken.Writable is false")
			log.Printf("[recover] failToken [%v] get token err! resGetToken.AllocId=%v", re.failToken, resGetToken.AllocId)
			//re.Upt.Delete(localTokenW)
			re.ReturnConTaskPass()
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
		re.IncFailToken()
		log.Printf("[recover:%d] failToken[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		//re.Upt.Delete(localTokenW)
		re.ReturnConTaskPass()
		return nil, err
	}
	log.Printf("[recover]get shard msg buf len(%d)\n", len(buf))

	if 0 == sw.swtoken {
		re.IncSuccToken()
		sw.swtoken++
	}

	re.IncConShard()
	shardBuf, err := clt.SendMsgClose(ctx, message.MsgIDDownloadShardRequest.Value(), buf)
	re.DecConShard()
	re.ReturnConTaskPass()

	if err != nil {
		if (strings.Contains(err.Error(),"Get data Slice fail")){
			re.IncFailShard()
			log.Printf("[recover:%d] failShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.failShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		}else{
			re.IncFailSendShard()
			log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		}
		//re.Upt.Delete(localTokenW)
		return nil, err
	}

	if len(shardBuf)<3{
		re.IncFailSendShard()
		log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		//re.Upt.Delete(localTokenW)
		return nil, err
	}

	err = proto.Unmarshal(shardBuf[2:], &res)
	if err != nil {
		re.IncFailSendShard()
		log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		//re.Upt.Delete(localTokenW)
		return nil, err
	}

	if 0 == sw.swshard {
		re.IncSuccShard()
		sw.swshard++
	}

	log.Printf("[recover:%d] successShard[%d] get shard [%s] success[%d]\n", BytesToInt64(btid[0:8]), re.successShard, base64.StdEncoding.EncodeToString(hash), *n)

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

func (re *RecoverEngine)processTask(ts *Task){
//	ts := req.Tsk
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

//func (re *RecoverEngine) Run2(){
////	for i := 0; i < max_concurrent_LRC; i++{
////		go processTask(re)
////	}
////
////	for {
////		re.MultiReply()
////	}
////}

func (re *RecoverEngine) Run() {
	go func() {
		for {
			ts := <-re.queue
			re.IncRbdTask()
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
				re.IncReportRbdTask()

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

	lrcshd := lrc
	can, err:=re.PreTstRecover(lrcshd,msg)
	if err != nil || !can {
		re.IncFailLessShard()
		return &res
	}

	re.IncPassJudge()
	//log.Println("[recover] pass recover test!")

	lrc.ShardExist = lrcshd.ShardExist

	h, err := re.le.GetLRCHandler(&lrc)
	if err != nil {
		log.Printf("[recover]LRC 获取Handler失败%s", err)
		return &res
	}

	log.Printf("[recover]lost idx %d: %s\n", lrc.Lostindex, base64.StdEncoding.EncodeToString(msg.Hashs[msg.RecoverId]))
	recoverData, err := h.Recover(msg)
	if err != nil {
		log.Printf("[recover]LRC 恢复失败%s", err)
		re.IncFailRbd()
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
		re.IncFailRbd()
		log.Printf("[recover]错误分片数据已保存 %s recoverID %x hash %s\n", BytesToInt64(msg.Id[0:8]), msg.RecoverId, base58.Encode(msg.Hashs[msg.RecoverId]))
		return &res
	}

	var key [common.HashLength]byte
	copy(key[:], hash)
	//if err := re.sn.YTFS().Put(common.IndexTableKey(key), recoverData); err != nil && err.Error() != "YTFS: hash key conflict happens" {
	if _, err := re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(key): recoverData}); err != nil && err.Error() != "YTFS: hash key conflict happens" {
		re.IncFailRbd()
		log.Printf("[recover]LRC 保存已恢复分片失败%s\n", err)
		return &res
	}

	re.IncSuccRbd()
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
		shard, err := re.getShard2(ctx, v.NodeId, base58.Encode(msg.Id), v.Addrs, msg.DataHash, &number)
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

