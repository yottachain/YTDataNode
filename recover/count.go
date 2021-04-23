package recover

import (
	"sync"
	"sync/atomic"
)

var (
	ConcurrentShardLk sync.Mutex
	ConCurrentTaskLK  sync.Mutex
	ConGetShardPoolLK sync.Mutex
)

func (re *RecoverEngine) IncSuccVersion() {
	atomic.AddUint64(&re.rcvstat.successVersion,1)
}

func (re *RecoverEngine) IncSendTokReq() {
	atomic.AddUint64(&re.rcvstat.sendTokenReq,1)
}

func (re *RecoverEngine) IncSuccPutTok() {
	atomic.AddUint64(&re.rcvstat.successPutToken,1)
}

func (re *RecoverEngine) IncGlobalRbdSucc() {
	atomic.AddUint64(&re.rcvstat.globalRebuildSucc,1)
}

func (re *RecoverEngine) IncColRbdSucc() {
	atomic.AddUint64(&re.rcvstat.columnRebuildSucc,1)
}

func (re *RecoverEngine) IncRowRbdSucc() {
	atomic.AddUint64(&re.rcvstat.rowRebuildSucc,1)
}

func (re *RecoverEngine) IncPreRbdSucc() {
	atomic.AddUint64(&re.rcvstat.preRebuildSucc,1)
}

func (re *RecoverEngine) IncShardForRbd() {
	atomic.AddUint64(&re.rcvstat.shardforRebuild,1)
}

func (re *RecoverEngine) IncSuccToken() {
	atomic.AddUint64(&re.rcvstat.successToken,1)
}

func (re *RecoverEngine) IncSuccConn() {
	atomic.AddUint64(&re.rcvstat.sucessConn,1)
}

func (re *RecoverEngine) IncPassJudge() {
	atomic.AddUint64(&re.rcvstat.passJudge,1)
}

func (re *RecoverEngine) IncFailLessShard() {
	atomic.AddUint64(&re.rcvstat.failLessShard,1)
}

func (re *RecoverEngine) GetConShardPass() {
	if DownloadCount.Len() > 0 {
		DownloadCount.Remove()
	}
}

func (re *RecoverEngine) ReturnConShardPass() {
	ConGetShardPoolLK.Lock()
	defer ConGetShardPoolLK.Unlock()
	DownloadCount.Add()
}

func (re *RecoverEngine) IncConTask() {
	ConCurrentTaskLK.Lock()
	defer ConCurrentTaskLK.Unlock()
	re.rcvstat.concurrentTask++
}

func (re *RecoverEngine) DecConTask() {
	ConCurrentTaskLK.Lock()
	defer ConCurrentTaskLK.Unlock()
	re.rcvstat.concurrentTask--
}

func (re *RecoverEngine) IncConShard() {
	ConcurrentShardLk.Lock()
	defer ConcurrentShardLk.Unlock()
	re.rcvstat.concurrenGetShard++
}

func (re *RecoverEngine) DecConShard() {
	ConcurrentShardLk.Lock()
	defer ConcurrentShardLk.Unlock()
	re.rcvstat.concurrenGetShard--
}

func (re *RecoverEngine) IncFailConn() {
	atomic.AddUint64(&re.rcvstat.failConn,1)
}

func (re *RecoverEngine) IncFailToken() {
	atomic.AddUint64(&re.rcvstat.failToken,1)
}

func (re *RecoverEngine) IncFailSendShard() {
	atomic.AddUint64(&re.rcvstat.failSendShard,1)
}

func (re *RecoverEngine) IncFailShard() {
	atomic.AddUint64(&re.rcvstat.failShard,1)
}

func (re *RecoverEngine) IncSuccShard() {
	atomic.AddUint64(&re.rcvstat.successShard,1)
}

func (re *RecoverEngine) IncFailDcdTask() {
	atomic.AddUint64(&re.rcvstat.failDecodeTaskID,1)
}

func (re *RecoverEngine) IncGetShardWK() {
	atomic.AddUint64(&re.rcvstat.getShardWkCnt,1)
}

func (re *RecoverEngine) IncRbdTask() {
	atomic.AddUint64(&re.rcvstat.rebuildTask,1)
}

func (re *RecoverEngine) IncReportRbdTask() {
	atomic.AddUint64(&re.rcvstat.reportTask,1)
}

func (re *RecoverEngine) IncSuccRbd() {
	atomic.AddUint64(&re.rcvstat.successRebuild,1)
}

func (re *RecoverEngine) IncFailRbd() {
	atomic.AddUint64(&re.rcvstat.failRebuild,1)
}

func (re *RecoverEngine) IncRbdSucc(n uint16) {
	if 0 == n {
		re.IncPreRbdSucc()
	}

	if 1 == n {
		re.IncRowRbdSucc()
	}

	if 2 == n {
		re.IncColRbdSucc()
	}

	if 3 <= n {
		re.IncGlobalRbdSucc()
	}
}
