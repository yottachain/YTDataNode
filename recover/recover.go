package recover

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/recover/actuator"
	"github.com/yottachain/YTDataNode/recover/shardDownloader"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTElkProducer"
	"github.com/yottachain/YTElkProducer/conf"
	"github.com/yottachain/YTHost/client"

	//"github.com/docker/docker/pkg/locker"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/reedsolomon"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/TokenPool"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	node "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTFS/common"
	lrcpkg "github.com/yottachain/YTLRC"
	_ "net/http/pprof"
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

type RecoverEngine struct {
	sn                node.StorageNode
	waitQueue         *TaskWaitQueue
	replyQueue        chan *TaskMsgResult
	le                *LRCEngine
	tstdata           [164]lrcpkg.Shard
	rcvstat           RebuildCount
	Upt               *TokenPool.TokenPool
	startTskTmCtl     uint8
	ElkClient         *YTElkProducer.Client
	DefaultDownloader shardDownloader.ShardDownloader
}

func New(sn node.StorageNode) (*RecoverEngine, error) {

	var re = new(RecoverEngine)
	re.waitQueue = NewTaskWaitQueue()
	re.replyQueue = make(chan *TaskMsgResult, max_reply_num)
	re.sn = sn
	re.DefaultDownloader = shardDownloader.New(sn.Host().ClientStore(), 5)
	re.le = NewLRCEngine(re.getShard, re.IncRbdSucc)
	re.Upt = TokenPool.Utp()
	logtb := sn.Config().BPMd5()
	tbstr := "dnlog-" + strings.ToLower(base58.Encode(logtb))
	re.ElkClient = NewElkClient(tbstr)

	return re, nil
}

func (re *RecoverEngine) Len() uint32 {
	return uint32(re.waitQueue.Len())
}

//RebuildTask = ReportTask    （近似相等）
//RebuildTask = SuccessRebuild + FailRebuild  （近似相等）
//GetShardWkCnt = FailDecodeTaskID + Success + FailShard + FailSendShard + FailToken + FailConn （近似相等）
//ConcurrentTask = ConcurrenGetShard （理想情况）

func (re *RecoverEngine) GetStat() *statistics.RecoverStat {
	return &statistics.RecoverStat{
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
		re.rcvstat.preRebuildSucc,
		re.rcvstat.successPutToken,
		re.rcvstat.sendTokenReq,
		re.rcvstat.successVersion,
		re.rcvstat.ackSuccRebuild,
		*RunningCount,
		*DownloadCount,
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
	return nil, nil //refer to getShard
}

func NewElkClient(tbstr string) *YTElkProducer.Client {
	elkConf := elasticsearch.Config{
		Addresses: []string{"https://c1-bj-elk.yottachain.net/"},
		Username:  "dnreporter",
		Password:  "dnreporter@yottachain",
	}

	ytESConfig := conf.YTESConfig{
		ESConf:      elkConf,
		DebugMode:   false,
		IndexPrefix: tbstr,
		IndexType:   "log",
	}

	client, _ := YTElkProducer.NewClient(ytESConfig)
	return &client
}

func (re *RecoverEngine) reportLog(body interface{}) {
	//if ! config.Gconfig.ElkReport{
	//	return
	//}

	if re.ElkClient == nil {
		log.Println("[recover][elk][error] no elkclient")
		return
	}
	(*re.ElkClient).AddLogAsync(body)
	time.Sleep(time.Second * 10)
}

func (re *RecoverEngine) MakeReportLog(nodeid string, hash []byte, errtype string, err error) *RcvDbgLog {
	//if ! config.Gconfig.ElkReport{
	//	return nil
	//}

	if re.ElkClient == nil {
		log.Println("[recover][elk][error] no elkclient")
		return nil
	}

	ShardId := base64.StdEncoding.EncodeToString(hash)
	NowTm := time.Now().Format("2006/01/02 15:04:05")
	localNodeId := re.sn.Config().ID
	localNdVersion := re.sn.Config().Version()
	return &RcvDbgLog{
		nodeid,
		ShardId,
		localNodeId,
		localNdVersion,
		NowTm,
		errtype,
		err.Error(),
	}
}

func (re *RecoverEngine) parmCheck(id string, taskID string, addrs []string, hash []byte, n *int, sw *Switchcnt) ([]byte, error) {
	if 0 == sw.swget {
		re.IncShardForRbd()
		sw.swget++
	}

	re.IncGetShardWK()
	btid, err := base58.Decode(taskID)
	if err != nil {
		re.IncFailDcdTask()
		return btid, err
	}

	if 0 == len(id) {
		err = fmt.Errorf("zero length id")
		re.IncFailDcdTask()
		return btid, err
	}

	if 0 == len(addrs) {
		err = fmt.Errorf("zero length addrs")
		re.IncFailDcdTask()
		return btid, err
	}

	if 0 == len(hash) {
		err = fmt.Errorf("zero length hash")
		re.IncFailDcdTask()
		return btid, err
	}
	return btid, nil
}

func (re *RecoverEngine) getRdToken(clt *client.YTHostClient, sw *Switchcnt) ([]byte, error) {
	var getToken message.NodeCapacityRequest
	getToken.RequestMsgID = message.MsgIDMultiTaskDescription.Value() + 1
	getTokenData, _ := proto.Marshal(&getToken)

	ctxto, cancels := context.WithTimeout(context.Background(), time.Second*10)
	defer cancels()
	tok, err := clt.SendMsg(ctxto, message.MsgIDNodeCapacityRequest.Value(), getTokenData)

	if err != nil {
		if config.Gconfig.ElkReport {
			//logelk:=re.MakeReportLog(id,hash,"failToken",err)
			//go re.reportLog(logelk)
		}

		return nil, err
	}

	if len(tok) < 3 {
		err = fmt.Errorf("the length of token less 3 byte")
		if config.Gconfig.ElkReport {
			//logelk:=re.MakeReportLog(id,hash,"failToken",err)
			//go re.reportLog(logelk)
		}
		return nil, err
	}
	return tok, err
}

func (re *RecoverEngine) getShardData(token, id, taskid string, addrs []string, hash []byte, n *int, sw *Switchcnt, clt *client.YTHostClient) ([]byte, error) {
	btid, _ := base58.Decode(taskid)
	var msg message.DownloadShardRequest
	msg.VHF = hash
	msg.AllocId = token

	buf, err := proto.Marshal(&msg)
	if err != nil {
		re.IncFailToken()
		log.Printf("[recover:%d] failToken[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		re.ReturnConShardPass()
		return nil, err
	}
	ctx2, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	re.IncConShard()
	shardBuf, err := clt.SendMsg(ctx2, message.MsgIDDownloadShardRequest.Value(), buf)
	re.DecConShard()
	if err != nil {
		if strings.Contains(err.Error(), "Get data Slice fail") {
			re.IncFailShard()
			if config.Gconfig.ElkReport {
				logelk := re.MakeReportLog(id, hash, "failShard", err)
				go re.reportLog(logelk)
			}
			log.Printf("[recover:%d] failShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.rcvstat.failShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		} else {
			re.IncFailSendShard()
			if config.Gconfig.ElkReport {
				logelk := re.MakeReportLog(id, hash, "failSendShard", err)
				go re.reportLog(logelk)
			}
			log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s addr %v\n", BytesToInt64(btid[0:8]), re.rcvstat.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs)
		}
		return nil, err
	}

	if len(shardBuf) < 3 {
		re.IncFailSendShard()
		log.Printf("[recover:%d] error: shard empty!! failSendShard[%v] get shard [%s] error[%d] addr %v\n", BytesToInt64(btid[0:8]), re.rcvstat.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, addrs)
		return nil, fmt.Errorf("error: shard less then 16384, len=", len(shardBuf))
	}
	return shardBuf, err
}

func (re *RecoverEngine) putToken(token string, clt *client.YTHostClient) {
	bkctxto, cancels2 := context.WithTimeout(context.Background(), time.Second*5)
	defer cancels2()

	var msgbck message.DownloadTKCheck
	msgbck.Tk = token
	buf, err := proto.Marshal(&msgbck)
	_, err = clt.SendMsg(bkctxto, message.MsgIDDownloadTKCheck.Value(), buf)
	if err != nil {
		log.Println("[recover] return token error,err=", err.Error())
	} else {
		re.IncSuccPutTok()
	}
}

func (re *RecoverEngine) getShard(id string, taskID string, addrs []string, hash []byte, n *int, sw *Switchcnt, tasklife int32) ([]byte, error) {
	btid, err := re.parmCheck(id, taskID, addrs, hash, n, sw)
	if err != nil {
		log.Println("[recover] parmcheck error :", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	clt, err := re.sn.Host().ClientStore().GetByAddrString(ctx, id, addrs)
	if err != nil {
		log.Println("[recover][debug] getShardcnn  err=", err)
		re.IncFailConn()
		if config.Gconfig.ElkReport {
			//logelk:=re.MakeReportLog(id,hash,"failConn",err)
			//go re.reportLog(logelk)
		}
		log.Printf("[recover:%d] failConn[%v] get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failConn, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
		return nil, err
	}

	if 0 == sw.swconn {
		re.IncSuccConn()
		sw.swconn++
	}

	//peerVersion := clt.RemotePeerVersion()
	//if int(peerVersion) < int(config.Gconfig.MinVersion) {
	//	err = fmt.Errorf("remote dn version is too low!")
	//	if config.Gconfig.ElkReport {
	//		//logelk := re.MakeReportLog(id, hash, "failVersion", err)
	//		//go re.reportLog(logelk)
	//	}
	//	return nil, err
	//}

	re.IncSuccVersion()
	re.GetConShardPass()

	//tok, err := re.getRdToken(clt, sw)
	//if err != nil {
	//	re.IncFailToken()
	//	log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
	//	re.ReturnConShardPass()
	//	return nil, err
	//
	//}

	//var resGetToken message.NodeCapacityResponse
	//err = proto.Unmarshal(tok[2:], &resGetToken)
	//if err != nil {
	//	re.IncFailToken()
	//	log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
	//	re.ReturnConShardPass()
	//	return nil, err
	//}

	//if !resGetToken.Writable {
	//	re.IncFailToken()
	//	err = fmt.Errorf("resGetToken.Writable is false")
	//	log.Printf("[recover:%d] failToken [%v] get token err! get shard [%s] error[%d] %s addr %v id %d \n", BytesToInt64(btid[0:8]), re.rcvstat.failToken, base64.StdEncoding.EncodeToString(hash), *n, err.Error(), addrs, id)
	//	re.ReturnConShardPass()
	//	return nil, err
	//}

	//if 0 == sw.swtoken {
	//	re.IncSuccToken()
	//	sw.swtoken++
	//}

	var res message.DownloadShardResponse
	shardBuf, err := re.getShardData("", id, taskID, addrs, hash, n, sw, clt)
	if err != nil {
		re.ReturnConShardPass()
		return nil, err
	}
	re.ReturnConShardPass()

	err = proto.Unmarshal(shardBuf[2:], &res)
	if err != nil {
		re.IncFailSendShard()
		log.Printf("[recover:%d] failSendShard[%v] get shard [%s] error[%d] %s\n", BytesToInt64(btid[0:8]), re.rcvstat.failSendShard, base64.StdEncoding.EncodeToString(hash), *n, err.Error())
		return nil, err
	}

	if 0 == sw.swshard {
		re.IncSuccShard()
		sw.swshard++
	}
	//
	//re.putToken(resGetToken.AllocId, clt)
	//log.Printf("[recover:%d] successShard[%d] get shard [%s] success[%d]\n", BytesToInt64(btid[0:8]), re.rcvstat.successShard, base64.StdEncoding.EncodeToString(hash), *n)

	*n = *n + 1
	return res.Data, nil
}

// TaskMsgResult 重建任务结果对象
type TaskMsgResult struct {
	ID          []byte // 重建任务ID
	RES         int32  // 重建任务结果 0：success 1：error
	BPID        int32  // 需要回复的BP的ID
	ExpriedTime int64  // 任务过期时间
	SrcNodeID   int32  // 来源节点ID
	ErrorMsg    error
}

// 多重建任务消息处理
func (re *RecoverEngine) HandleMuilteTaskMsg(msgData []byte) error {
	var mtdMsg message.MultiTaskDescription
	if err := proto.Unmarshal(msgData, &mtdMsg); err != nil {
		return err
	}
	for _, task := range mtdMsg.Tasklist {
		bys := task[12:14]
		bytebuff := bytes.NewBuffer(bys)
		var snID uint16
		binary.Read(bytebuff, binary.BigEndian, &snID)

		// 如果大于过期时间跳过
		//if time.Now().Unix() > mtdMsg.ExpiredTime {
		//	continue
		//}

		if err := re.waitQueue.PutTask(task, int32(snID), mtdMsg.ExpiredTime, mtdMsg.SrcNodeID, mtdMsg.ExpiredTimeGap); err != nil {
			log.Printf("[recover]put recover task error: %s\n", err.Error())
		}
	}
	return nil
}

/**
 * @Description: 分发不同类型的任务给不同执行器，目前只考虑LRC任务
 * @receiver re
 * @param ts 任务
 * @param pkgstart 任务包开始时间
 */
func (re *RecoverEngine) dispatchTask(ts *Task, pkgstart time.Time) {
	var msgID int16
	binary.Read(bytes.NewBuffer(ts.Data[:2]), binary.BigEndian, &msgID)
	var res *TaskMsgResult

	switch int32(msgID) {
	case message.MsgIDLRCTaskDescription.Value():
		res = re.execLRCTask(ts.Data[2:], ts.ExpriedTime, pkgstart, ts.TaskLife)
		if res.ErrorMsg != nil {
			log.Println("[recover]", res.ErrorMsg)
			res.RES = 1
		}
		res.BPID = ts.SnID
		res.SrcNodeID = ts.SrcNodeID
		re.PutReplyQueue(res)
	case message.MsgIDTaskDescriptCP.Value():
		//res = re.execCPTask(ts.Data[2:], ts.ExpriedTime)
	default:
		log.Println("[recover] unknown msgID")
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
				_r.SrcNodeID = res.SrcNodeID
				resmsg[res.BPID] = _r
				re.IncReportRbdTask()

			case <-time.After(max_reply_wait_time):
				return
			}
		}
	}()
	for k, v := range resmsg {
		v.NodeID = int32(re.sn.Config().IndexID)
		if data, err := proto.Marshal(v); err != nil {
			log.Printf("[recover][report] marsnal failed %s\n", err.Error())
			continue
		} else {
			reportTms := 6
			for {
				reportTms--
				if 0 >= reportTms {
					log.Println("[recover][report]Send msg error: ", err)
					break
				}
				resp, err := re.sn.SendBPMsg(int(k), message.MsgIDMultiTaskOPResult.Value(), data)
				if err == nil {
					if len(resp) < 3 {
						continue
					}

					var res message.MultiTaskOpResultRes
					err = proto.Unmarshal(resp[2:], &res)
					if err != nil {
						continue
					} else {
						if 0 == res.ErrCode {
							re.rcvstat.ackSuccRebuild += uint64(res.SuccNum)
						}
					}
					log.Println("[recover][report] multi reply success, rebuildTask=", re.rcvstat.rebuildTask, "reportTask=", re.rcvstat.reportTask, "ackSuccRebuild", re.rcvstat.ackSuccRebuild)
					break
				}
			}
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

type PreJudgeReport struct {
	LocalNdID  string
	LostHash   string
	LostIndex  uint16
	FailType   string
	ShardExist string
}

func (re *RecoverEngine) MakeJudgeElkReport(lrcShd *lrcpkg.Shardsinfo, msg message.TaskDescription) *PreJudgeReport {
	//if ! config.Gconfig.ElkReport{
	//	return nil
	//}
	if re.ElkClient == nil {
		log.Println("[recover][elk] error,no client")
		return nil
	}
	localid := re.sn.Config().ID
	lostidx := lrcShd.Lostindex
	losthash := base64.StdEncoding.EncodeToString(msg.Hashs[msg.RecoverId])
	failtype := "failJudge"
	shardExist := lrcShd.ShardExist[:164]
	shdExistStr := make([]string, len(shardExist))
	for k, v := range shardExist {
		shdExistStr[k] = fmt.Sprintf("%d", v)
	}
	strExist := strings.Join(shdExistStr, "")
	return &PreJudgeReport{
		LocalNdID:  localid,
		LostHash:   losthash,
		LostIndex:  lostidx,
		FailType:   failtype,
		ShardExist: strExist,
	}
}

/**
 * @Description: 用任务消息初始化LRC任务句柄
 * @receiver re
 * @param msg
 * @return *LRCHandler
 * @return error
 */
func (re *RecoverEngine) initLRCHandlerByMsg(msg message.TaskDescription) (*LRCHandler, error) {
	lrc := &lrcpkg.Shardsinfo{}

	lrc.OriginalCount = uint16(len(msg.Hashs) - int(msg.ParityShardCount))
	lrc.RecoverNum = 13
	lrc.Lostindex = uint16(msg.RecoverId)
	return re.le.GetLRCHandler(lrc)
}

/**
 * @Description: 验证重建后的数据并保存
 * @receiver re
 * @param recoverData
 * @param msg
 * @param res
 * @return *TaskMsgResult
 */
func (re *RecoverEngine) verifyLRCRecoveredDataAndSave(recoverData []byte, msg message.TaskDescription, res *TaskMsgResult) error {
	hashBytes := md5.Sum(recoverData)
	hash := hashBytes[:]

	if !bytes.Equal(hash, msg.Hashs[msg.RecoverId]) {
		re.IncFailRbd()

		return fmt.Errorf(
			"[recover]fail shard saved %s recoverID %x hash %s\n",
			BytesToInt64(msg.Id[0:8]),
			msg.RecoverId,
			base58.Encode(msg.Hashs[msg.RecoverId]),
		)
	}

	var key [common.HashLength]byte
	copy(key[:], hash)

	if _, err := re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(key): recoverData}); err != nil && err.Error() != "YTFS: hash key conflict happens" {
		re.IncFailRbd()
		return fmt.Errorf("[recover]LRC recover shard saved failed%s\n", err)
	}
	return nil
}

/**
 * @Description: 执行lrc 重建任务
 * @receiver re
 * @param msgData 单个重建消息
 * @param expried 过期时间
 * @param pkgstart 任务包开始时间
 * @param tasklife 任务存活周期
 * @return *TaskMsgResult 任务执行结果
 */
func (re *RecoverEngine) execLRCTask(msgData []byte, expired int64, pkgStart time.Time, taskLife int32) (res *TaskMsgResult) {
	// @TODO 初始化返回
	res = &TaskMsgResult{}
	res.ExpriedTime = expired
	res.RES = 1
	taskActuator := actuator.New(re.DefaultDownloader)

	var recoverData []byte
	// @TODO 执行恢复任务
	for _, opts := range []actuator.Options{
		//actuator.Options{
		//	Expired: time.Now().Add(time.Minute * 5),
		//	Stage:   actuator.RECOVER_STAGE_BASE,
		//},
		actuator.Options{
			Expired: time.Now().Add(time.Minute * 5),
			Stage:   actuator.RECOVER_STAGE_ROW,
		},
		actuator.Options{
			Expired: time.Now().Add(time.Minute * 5),
			Stage:   actuator.RECOVER_STAGE_COL,
		},
		actuator.Options{
			Expired: time.Now().Add(time.Minute * 5),
			Stage:   actuator.RECOVER_STAGE_FULL,
		},
	} {

		startTime := time.Now()
		data, resID, err := taskActuator.ExecTask(
			msgData,
			opts,
		)
		res.ID = resID
		// @TODO 如果重建成功退出循环
		if err == nil && data != nil {
			recoverData = data
			break
		}
		fmt.Printf("[recover] %d 阶段耗时 %f s 失败: %s\n", opts.Stage, time.Now().Sub(startTime).Seconds(), err.Error())
	}

	if recoverData == nil {
		res.ErrorMsg = fmt.Errorf("all rebuild stage fail")
		res.RES = 1
		return
	}

	// @TODO 存储重建完成的分片
	hashBytes := md5.Sum(recoverData)
	hash := hashBytes[:]
	var key [common.HashLength]byte
	copy(key[:], hash)
	if _, err := re.sn.YTFS().BatchPut(map[common.IndexTableKey][]byte{common.IndexTableKey(key): recoverData}); err != nil && err.Error() != "YTFS: hash key conflict happens" {
		res.ErrorMsg = fmt.Errorf("[recover]LRC recover shard saved failed%s\n", err)
		return
	}

	res.RES = 0
	return
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
