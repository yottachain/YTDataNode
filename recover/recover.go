package recover

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/yottachain/YTElkProducer"
	"github.com/yottachain/YTElkProducer/conf"

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
	"strings"
	"sync"
	"time"
	//"io"
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

type RebuildCount struct {
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
	shardforRebuild  uint64
	rowRebuildSucc   uint64
	columnRebuildSucc  uint64
	globalRebuildSucc  uint64
	successPutToken    uint16
}

type RecoverEngine struct {
	sn           node.StorageNode
	queue        chan *Task
	replyQueue   chan *TaskMsgResult
	le           *LRCEngine
	tstdata      [164]lrcpkg.Shard
	rcvstat          RebuildCount
	Upt             *TaskPool.TaskPool
	startTskTmCtl    uint8
}

type RcvDbgLog struct {
	Nodeid   string
	Shardid  string
	Time     string
	Errtype  string
	Errstr   string
}

func New(sn node.StorageNode) (*RecoverEngine, error) {
	var re = new(RecoverEngine)
	re.queue = make(chan *Task, max_task_num)
	re.replyQueue = make(chan *TaskMsgResult, max_reply_num)
	re.sn = sn
	re.le = NewLRCEngine(re.getShard ,re.IncRbdSucc)
    re.Upt = TaskPool.Utp()
	return re, nil
}

func (re *RecoverEngine) Len() uint32 {
	return uint32(len(re.queue))
}

type RecoverStat struct {
	RebuildTask     uint64  `json:"RebuildTask"`                         //下发重建的任务总数
	ConcurrentTask  uint64   `json:"ConcurrentTask"`                     //并发进行的重建任务数
	ConcurrenGetShard  uint64   `json:"ConcurenGetShard"`                //并发拉取分片数
	SuccessRebuild  uint64  `json:"SuccessRebuild"`                      //成功的重建的任务总数
	FailRebuild  uint64   `json:"FailRebuild"`                           //重建失败的任务总数
	ReportTask   uint64    `json:"ReportTask"`                           //上报的任务总数（包括重建成功和失败的上报）
	GetShardWkCnt   uint64  `json:"getShardWkCnt"`                       //拉取分片的总次数
	FailDecodeTaskID uint64   `json:"failDecodeTaskID"`                  //拉取分片时，解码需拉取的分片信息的错误总数（有可能需拉取的分片信息不全）
	Success   uint64  `json:"Success"`                                   //成功拉取的分片总数
	FailShard uint64  `json:"FailShard"`                                 //不存在的总分片数（分片丢失）
	FailSendShard  uint64  `json:"FailSendShard"`                        //分片存在，但是传输过程中分片丢失
	FailToken uint64  `json:"FailToken"`                                 //拉取分片时，获取不到token的总次数
	FailConn  uint64  `json:"failConn"`                                  //拉取分片时，无法连接的总数
	FailLessShard   uint64   `json:"failLessShard"`                       //在线矿机数不够，无法获取足够分片
	PassJudge       uint64   `json:"passJudge"`                           //预判重建成功
	SuccessConn      uint64  `json:"sucessConn"`                          //连接成功数
	SuccessToken    uint64   `json:"successToken"`                        //获取token成功
	ShardforRebuild  uint64  `json:"shardforRebuild"`                     //下载总分片数
	RowRebuildSucc   uint64  `json:"RowRebuildSucc"`                      //行方式重建成功
	ColumnRebuildSucc  uint64  `json:"ColumnRebuildSucc"`                 //列方式重建成功
	GlobalRebuildSucc  uint64   `json:"GlobalRebuildSucc"`                //全局方式重建成功
	SuccessPutToken    uint16   `json:"SuccessPutToken"`                  //成功释放token总数
}

//RebuildTask = ReportTask    （近似相等）
//RebuildTask = SuccessRebuild + FailRebuild  （近似相等）
//GetShardWkCnt = FailDecodeTaskID + Success + FailShard + FailSendShard + FailToken + FailConn （近似相等）
//ConcurrentTask = ConcurrenGetShard （理想情况）

func (re *RecoverEngine) GetStat() *RecoverStat {
	return &RecoverStat{
		re.rcvstat.rebuildTask,
		re.rcvstat.concurrentTask,
		re.rcvstat.concurrenGetShard,
		re.rcvstat.successRebuild,
		re.rcvstat.failRebuild,
		re.rcvstat.getShardWkCnt,
		re.rcvstat.reportTask,
		re.rcvstat.failDecodeTaskID,
		re.rcvstat.successShard,
		re.rcvstat.failShard,
		re.rcvstat.failSendShard,
		re.rcvstat.failToken,
		re.rcvstat.failConn,
		re.rcvstat.failLessShard,
		re.rcvstat.passJudge,
		re.rcvstat.sucessConn,
		re.rcvstat.successToken,
		re.rcvstat.shardforRebuild,
		re.rcvstat.rowRebuildSucc,
		re.rcvstat.columnRebuildSucc,
		re.rcvstat.globalRebuildSucc,
		re.rcvstat.successPutToken,
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

func (re *RecoverEngine) parmCheck(id string, taskID string, addrs []string, hash []byte, n *int,sw *Switchcnt) ([]byte,error){
	if 0 == sw.swget{
		re.IncShardForRbd()
		sw.swget++
	}

	re.IncGetShardWK()
	btid, err := base58.Decode(taskID)
	if err != nil {
		re.IncFailDcdTask()
		return btid,err
	}

	if 0 == len(id) {
		err = fmt.Errorf("zero length id")
		re.IncFailDcdTask()
		return  btid,err
	}

	if 0 == len(addrs) {
		err = fmt.Errorf("zero length addrs")
		re.IncFailDcdTask()
		return  btid,err
	}

	if 0 == len(hash) {
		err = fmt.Errorf("zero length hash")
		re.IncFailDcdTask()
		return  btid,err
	}
	return btid, nil
}

//func (re *RecoverEngine) reportLog( body interface{}){
//	bodyBytes, err:=json.Marshal(body)
//	resp, err := http.Post(url, contentType, bytes.NewReader(bodyBytes))
//	if err != nil {
//		panic(err)
//	}
//	//关闭连接
//	defer resp.Body.Close()
//}

func (re *RecoverEngine) reportLog(body *RcvDbgLog){
	elkConf := elasticsearch.Config{
		Addresses: []string{"https://c1-bj-elk.yottachain.net/"},
		Username:  "dnreporter",
		Password:  "dnreporter@yottachain",
	}

	ytESConfig := conf.YTESConfig{
		ESConf:      elkConf,
		DebugMode:   true,
		IndexPrefix: "main-net-dn",
		IndexType:   "log",
	}

	client := YTElkProducer.NewClient(ytESConfig)

	client.AddLogAsync(body)
	time.Sleep(time.Second*10)
}

func (re *RecoverEngine) MakeReportLog(nodeid string, hash []byte, errtype string,  err error) *RcvDbgLog{
	ShardId := base64.StdEncoding.EncodeToString(hash)
	NowTm := time.Now().Format("2006/01/02 15:04:05")
	return &RcvDbgLog{
		nodeid,
		ShardId,
		NowTm,
		errtype,
		err.Error(),
	}
}

func (re *RecoverEngine) getShard( id string, taskID string, addrs []string, hash []byte, n *int,sw *Switchcnt) ([]byte, error) {

	btid,err:=re.parmCheck(id, taskID, addrs, hash, n, sw)

	if err !=nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	//connStart := time.Now()

	clt, err := re.sn.Host().ClientStore().GetByAddrString(ctx, id, addrs)
	if err != nil {
		re.IncFailConn()
		logelk:=re.MakeReportLog(id,hash,"failConn",err)
		go re.reportLog(logelk)
		log.Printf("[recover:%d] failConn[%v] get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failConn, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
		return nil, err
	}

	if 0 == sw.swconn {
		re.IncSuccConn()
		sw.swconn++
	}

/*********************************************
	var getToken message.NodeCapacityRequest
	var resGetToken message.NodeCapacityResponse
	getToken.RequestMsgID = message.MsgIDMultiTaskDescription.Value()
	getTokenData, _ := proto.Marshal(&getToken)

	re.GetConShardPass()
	ctxto, cancels := context.WithTimeout(context.Background(), time.Second*10)
	defer cancels()
**********************************************/


/*************************************************
	tok, err := clt.SendMsg(ctxto, message.MsgIDNodeCapacityRequest.Value(), getTokenData)

	if err != nil {
		re.IncFailToken()
		log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)

		re.ReturnConShardPass()
		return nil,err
	}

    if len(tok) < 3{
		err = fmt.Errorf("the length of token less 3 byte")
		log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n,  addrs, id)
		re.IncFailToken()
		re.ReturnConShardPass()
		return nil,err
	}

	err = proto.Unmarshal(tok[2:], &resGetToken)
	if err != nil {
		re.IncFailToken()
		log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
		re.ReturnConShardPass()
		return nil,err
	}

	if !resGetToken.Writable {
		re.IncFailToken()
		err = fmt.Errorf("resGetToken.Writable is false")
		log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
		re.ReturnConShardPass()
		return nil,err
	}

	var msg message.DownloadShardRequest
	var res message.DownloadShardResponse
	msg.VHF = hash
	msg.AllocId = resGetToken.AllocId

	buf, err := proto.Marshal(&msg)
	if err != nil {
		re.IncFailToken()
		log.Printf("[recover:%d] failToken[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		re.ReturnConShardPass()
		return nil, err
	}
	log.Printf("[recover]get shard msg buf len(%d)\n", len(buf))
**************************************/
	if 0 == sw.swtoken {
		re.IncSuccToken()
		sw.swtoken++
	}

	var msg message.DownloadShardRequest
	//var res message.DownloadShardResponse

	msg.VHF = hash
	buf, err := proto.Marshal(&msg)
	if err != nil {
		//re.IncFailToken()
		log.Printf("[recover:%d] failToken[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		re.ReturnConShardPass()
		return nil, err
	}

	var res message.DownloadShardResponse
	ctx2, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	re.IncConShard()
	shardBuf, err := clt.SendMsg(ctx2, message.MsgIDDownloadShardRequest.Value(), buf)
	//shardBuf, err := clt.SendMsg(ctx2, message.MsgIDDownloadShardRequest.Value(), buf)
	//shardBuf, err := clt.SendMsgClose(ctx2, message.MsgIDDownloadShardRequest.Value(), nil)
	re.DecConShard()
	re.ReturnConShardPass()

	if err != nil {
		if (strings.Contains(err.Error(),"Get data Slice fail")){
			re.IncFailShard()
			logelk:=re.MakeReportLog(id,hash,"failShard",err)
			go re.reportLog(logelk)
			log.Printf("[recover:%d] failShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.rcvstat.failShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		}else{
			re.IncFailSendShard()
			logelk:=re.MakeReportLog(id,hash,"failSendShard",err)
			go re.reportLog(logelk)
			log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.rcvstat.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		}
		return nil, err
	}

	if len(shardBuf)<3{
		re.IncFailSendShard()
		log.Printf("[recover:%d] error: shard empty!! failSendShard[%v] get shard [%s] error[%d] addr %v\n", BytesToInt64(btid[0:8]), re.rcvstat.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, addrs)
		return nil, fmt.Errorf("error: shard less then 16384, len=",len(shardBuf))
	}

	err = proto.Unmarshal(shardBuf[2:], &res)
	if err != nil {
		re.IncFailSendShard()
		log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.rcvstat.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		return nil, err
	}

/***************************************
	bkctxto, cancels2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancels2()

    var msgbck message.DownloadTKCheck
    msgbck.Tk = resGetToken.AllocId
    buf,err = proto.Marshal(&msgbck)

    _,err = clt.SendMsg(bkctxto,message.MsgIDDownloadTKCheck.Value(),buf)
    if err != nil{
    	log.Println("[recover] return token error,err=",err.Error())
	}else{
		re.IncSuccPutTok()
		log.Println("[recover] return token Success,successPutTok=",re.rcvstat.successPutToken)
	}
 *************************************************/
	if 0 == sw.swshard {
		re.IncSuccShard()
		sw.swshard++
	}

	log.Printf("[recover:%d] successShard[%d] get shard [%s] success[%d]\n", BytesToInt64(btid[0:8]), re.rcvstat.successShard, base64.StdEncoding.EncodeToString(hash), *n)

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

func (re *RecoverEngine)processTask(ts *Task, pkgstart time.Time){
//	ts := req.Tsk
	msg := ts.Data
	if bytes.Equal(msg[0:2], message.MsgIDTaskDescript.Bytes()) {
		res := re.execRCTask(msg[2:], ts.ExpriedTime)
		res.BPID = ts.SnID
		re.PutReplyQueue(res)
	} else if bytes.Equal(msg[0:2], message.MsgIDLRCTaskDescription.Bytes()) {
		log.Printf("[recover]LRC start\n")
		res := re.execLRCTask(msg[2:], ts.ExpriedTime, pkgstart)
		res.BPID = ts.SnID
		log.Println("[recover] putres_to_queue, check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
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
	startTsk := time.Now()
	go func() {
		for {
			ts := <-re.queue
			if 0 == re.startTskTmCtl {
				startTsk = time.Now()
				log.Println("[recover] task_package start_time=",time.Now().Unix(),"len=",len(re.queue)+1)
				re.startTskTmCtl++
			}

			if time.Now().Sub(startTsk).Seconds() > (1800-60){
				if len(re.queue) <= 0{
					log.Println("[recover] task_package now_time_expired=",time.Now().Unix(),"len=",len(re.queue)+1)
					re.startTskTmCtl = 0
				}
				continue
			}

			re.IncRbdTask()
			msg := ts.Data
			if bytes.Equal(msg[0:2], message.MsgIDTaskDescript.Bytes()) {
				res := re.execRCTask(msg[2:], ts.ExpriedTime)
				res.BPID = ts.SnID
				re.PutReplyQueue(res)
			} else if bytes.Equal(msg[0:2], message.MsgIDLRCTaskDescription.Bytes()) {
				log.Printf("[recover]LRC start\n")
				res := re.execLRCTask(msg[2:], ts.ExpriedTime, startTsk)
				res.BPID = ts.SnID
				log.Println("[recover][report] res=",res.BPID,"reportTask=",re.rcvstat.reportTask,"rebuildTask=",re.rcvstat.rebuildTask)
				re.PutReplyQueue(res)
			} else {
				res := re.execCPTask(msg[2:], ts.ExpriedTime)
				res.BPID = ts.SnID
				re.PutReplyQueue(res)
			}
			if len(re.queue) <= 0{
				re.startTskTmCtl = 0
				log.Println("[recover] task_package now_time_que_empty=",time.Now().Unix(),"len=",len(re.queue)+1)
				continue
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
				log.Println("[recover][report] get_res_replyQueue len(resmsg)=",len(re.replyQueue))
				if resmsg[res.BPID] == nil {
					resmsg[res.BPID] = &message.MultiTaskOpResult{}
				}
				_r := resmsg[res.BPID]

				_r.Id = append(_r.Id, res.ID)
				_r.RES = append(_r.RES, res.RES)
				_r.ExpiredTime = res.ExpriedTime
				resmsg[res.BPID] = _r
				log.Println("[recover][report] put_res_to_resmsg len(resmsg)=",len(resmsg))
				re.IncReportRbdTask()

			case <-time.After(max_reply_wait_time):
				return
			}
		}
	}()
	if l := len(resmsg); l > 0 {
		fmt.Println("[recover][report]  len(resmsg)=", len(resmsg))
	}
	for k, v := range resmsg {
		v.NodeID = int32(re.sn.Config().IndexID)
		if data, err := proto.Marshal(v); err != nil {
			log.Printf("[recover][report] marsnal failed %s\n", err.Error())
			continue
		} else {
			re.sn.SendBPMsg(int(k), message.MsgIDMultiTaskOPResult.Value(), data)
			log.Printf("[recover][report] multi reply success nodeID %d, expried %d\n", v.NodeID, v.ExpiredTime)
		    log.Println("[recover][report] rebuildTask=",re.rcvstat.rebuildTask,"reportTask=",re.rcvstat.reportTask)
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

func (re *RecoverEngine) execLRCTask(msgData []byte, expried int64, pkgstart time.Time ) *TaskMsgResult {

	var res TaskMsgResult
	res.ExpriedTime = expried
	var msg message.TaskDescription

	if err := proto.Unmarshal(msgData, &msg); err != nil {
		log.Printf("[recover]proto check error:%s", err)
		res.RES = 1
	}

	res.ID = msg.Id
	res.RES = 1
	log.Printf("[recover]LRC recover shard start%d", BytesToInt64(msg.Id[0:8]))
	defer log.Printf("[recover]LRC recover shard end %d", BytesToInt64(msg.Id[0:8]))

	lrc := &lrcpkg.Shardsinfo{}
	lrcshd := &lrcpkg.Shardsinfo{}

	lrc.OriginalCount = uint16(len(msg.Hashs) - int(msg.ParityShardCount))
	log.Printf("[recover]LRC original count is %d", lrc.OriginalCount)
	lrc.RecoverNum = 13
	lrc.Lostindex = uint16(msg.RecoverId)

	lrcshd = lrc
	can, err:=re.PreTstRecover(lrcshd,msg)
	if err != nil || !can {
		re.IncFailLessShard()
		return &res
	}

	//if time.Now().Sub(pkgstart).Seconds() > 1800 -60 {
	//	log.Println("[recover] rebuild time expired!")
	//	return &res
	//}

	re.IncPassJudge()
	//log.Println("[recover] pass recover test!")

	lrc.ShardExist = lrcshd.ShardExist
    //log.Println("[recover] prelrcshd=",lrcshd)
	//log.Println("[recover] lrc=",lrc)

	h, err := re.le.GetLRCHandler(lrc)
	if err != nil {
		log.Printf("[recover]LRC get Handler failed %s", err)
		return &res
	}

	log.Printf("[recover]lost idx %d: %s\n", lrc.Lostindex, base64.StdEncoding.EncodeToString(msg.Hashs[msg.RecoverId]))
	recoverData, err := h.Recover(msg, pkgstart)
	if err != nil {
		log.Println("[recover] check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
		log.Printf("[recover]LRC recover shard failed %s", err)
		re.IncFailRbd()
		return &res
	}

	log.Printf("[recover]LRC recover data: %s", hex.EncodeToString(recoverData[0:128]))
	m5 := md5.New()
	m5.Write(recoverData)
	hash := m5.Sum(nil)
	// 校验hash失败
	if !bytes.Equal(hash, msg.Hashs[msg.RecoverId]) {
		log.Printf("[recover]LRC verify HASH failed %s %s\n", base58.Encode(hash), base58.Encode(msg.Hashs[msg.RecoverId]))
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
		log.Printf("[recover]fail shard saved %s recoverID %x hash %s\n", BytesToInt64(msg.Id[0:8]), msg.RecoverId, base58.Encode(msg.Hashs[msg.RecoverId]))
		return &res
	}

	var key [common.HashLength]byte
	copy(key[:], hash)
	//if err := re.sn.YTFS().Put(common.IndexTableKey(key), recoverData); err != nil && err.Error() != "YTFS: hash key conflict happens" {
	if _, err := re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(key): recoverData}); err != nil && err.Error() != "YTFS: hash key conflict happens" {
		re.IncFailRbd()
		log.Printf("[recover]LRC recover shard saved failed%s\n", err)
		return &res
	}

	//if time.Now().Sub(pkgstart).Seconds() > 1800{
	//	log.Println("[recover] rebuild time expired!")
	//	return &res
	//}

	re.IncSuccRbd()
	log.Println("[recover] check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
	log.Printf("[recover]LRC shard recover success\n")
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

