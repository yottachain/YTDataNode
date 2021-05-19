package statistics

import (
	"sync"
	"sync/atomic"
)

var (
	ConcurrentShardLk sync.Mutex
	ConCurrentTaskLK  sync.Mutex
	ConGetShardPoolLK sync.Mutex
)
var DefaultRebuildCount RebuildCount
var DownloadCount *WaitCount
var RunningCount *WaitCount

type RebuildCount struct {
	rebuildTask        uint64
	concurrentTask     uint64
	concurrenGetShard  uint64
	successRebuild     uint64
	failRebuild        uint64
	reportTask         uint64
	getShardWkCnt      uint64
	failDecodeTaskID   uint64
	successShard       uint64
	failShard          uint64
	failSendShard      uint64
	failToken          uint64
	failConn           uint64
	failLessShard      uint64
	passJudge          uint64
	sucessConn         uint64
	successToken       uint64
	shardforRebuild    uint64
	rowRebuildSucc     uint64
	columnRebuildSucc  uint64
	globalRebuildSucc  uint64
	preRebuildSucc     uint64
	successPutToken    uint64
	sendTokenReq       uint64
	successVersion     uint64
	ackSuccRebuild     uint64
	RowRebuildCount    uint64
	ColRebuildCount    uint64
	GlobalRebuildCount uint64
}

func (rc *RebuildCount) IncSuccVersion() {
	atomic.AddUint64(&rc.successVersion, 1)
}

func (rc *RebuildCount) IncSendTokReq() {
	atomic.AddUint64(&rc.sendTokenReq, 1)
}

func (rc *RebuildCount) IncSuccPutTok() {
	atomic.AddUint64(&rc.successPutToken, 1)
}

func (rc *RebuildCount) IncGlobalRbdSucc() {
	atomic.AddUint64(&rc.globalRebuildSucc, 1)
}

func (rc *RebuildCount) IncColRbdSucc() {
	atomic.AddUint64(&rc.columnRebuildSucc, 1)
}

func (rc *RebuildCount) IncRowRbdSucc() {
	atomic.AddUint64(&rc.rowRebuildSucc, 1)
}

func (rc *RebuildCount) IncPreRbdSucc() {
	atomic.AddUint64(&rc.preRebuildSucc, 1)
}

func (rc *RebuildCount) IncShardForRbd() {
	atomic.AddUint64(&rc.shardforRebuild, 1)
}

func (rc *RebuildCount) IncSuccToken() {
	atomic.AddUint64(&rc.successToken, 1)
}

func (rc *RebuildCount) IncSuccConn() {
	atomic.AddUint64(&rc.sucessConn, 1)
}

func (rc *RebuildCount) IncPassJudge() {
	atomic.AddUint64(&rc.passJudge, 1)
}

func (rc *RebuildCount) IncFailLessShard() {
	atomic.AddUint64(&rc.failLessShard, 1)
}

func (rc *RebuildCount) GetConShardPass() {
	if DownloadCount.Len() > 0 {
		DownloadCount.Remove()
	}
}

func (rc *RebuildCount) ReturnConShardPass() {
	ConGetShardPoolLK.Lock()
	defer ConGetShardPoolLK.Unlock()
	DownloadCount.Add()
}

func (rc *RebuildCount) IncConTask() {
	ConCurrentTaskLK.Lock()
	defer ConCurrentTaskLK.Unlock()
	rc.concurrentTask++
}

func (rc *RebuildCount) DecConTask() {
	ConCurrentTaskLK.Lock()
	defer ConCurrentTaskLK.Unlock()
	rc.concurrentTask--
}

func (rc *RebuildCount) IncConShard() {
	ConcurrentShardLk.Lock()
	defer ConcurrentShardLk.Unlock()
	rc.concurrenGetShard++
}

func (rc *RebuildCount) DecConShard() {
	ConcurrentShardLk.Lock()
	defer ConcurrentShardLk.Unlock()
	rc.concurrenGetShard--
}

func (rc *RebuildCount) IncFailConn() {
	atomic.AddUint64(&rc.failConn, 1)
}

func (rc *RebuildCount) IncFailToken() {
	atomic.AddUint64(&rc.failToken, 1)
}

func (rc *RebuildCount) IncFailSendShard() {
	atomic.AddUint64(&rc.failSendShard, 1)
}

func (rc *RebuildCount) IncFailShard() {
	atomic.AddUint64(&rc.failShard, 1)
}

func (rc *RebuildCount) IncSuccShard() {
	atomic.AddUint64(&rc.successShard, 1)
}

func (rc *RebuildCount) IncFailDcdTask() {
	atomic.AddUint64(&rc.failDecodeTaskID, 1)
}

func (rc *RebuildCount) IncGetShardWK() {
	atomic.AddUint64(&rc.getShardWkCnt, 1)
}

func (rc *RebuildCount) IncRbdTask() {
	atomic.AddUint64(&rc.rebuildTask, 1)
}

func (rc *RebuildCount) IncReportRbdTask() {
	atomic.AddUint64(&rc.reportTask, 1)
}

func (rc *RebuildCount) IncSuccRbd() {
	atomic.AddUint64(&rc.successRebuild, 1)
}

func (rc *RebuildCount) IncFailRbd() {
	atomic.AddUint64(&rc.failRebuild, 1)
}

func (rc *RebuildCount) IncRbdSucc(n uint16) {
	if 0 == n {
		rc.IncPreRbdSucc()
	}

	if 1 == n {
		rc.IncRowRbdSucc()
	}

	if 2 == n {
		rc.IncColRbdSucc()
	}

	if 3 <= n {
		rc.IncGlobalRbdSucc()
	}
}
func (rc *RebuildCount) IncAckSuccRebuild(num uint64) {
	atomic.AddUint64(&rc.ackSuccRebuild, num)
}

func (rc *RebuildCount) GetStat() *RecoverStat {
	return &RecoverStat{
		rc.rebuildTask,
		rc.concurrentTask,
		rc.concurrenGetShard,
		rc.successRebuild,
		rc.failRebuild,
		rc.getShardWkCnt,
		rc.reportTask,
		rc.failDecodeTaskID,
		rc.successShard,
		rc.failShard,
		rc.failSendShard,
		rc.failToken,
		rc.failConn,
		rc.failLessShard,
		rc.passJudge,
		rc.sucessConn,
		rc.successToken,
		rc.shardforRebuild,
		rc.rowRebuildSucc,
		rc.columnRebuildSucc,
		rc.globalRebuildSucc,
		rc.preRebuildSucc,
		rc.successPutToken,
		rc.sendTokenReq,
		rc.successVersion,
		rc.ackSuccRebuild,
		rc.concurrentTask,
		rc.concurrenGetShard,
		rc.RowRebuildCount,
		rc.ColRebuildCount,
		rc.GlobalRebuildCount,
	}
}
