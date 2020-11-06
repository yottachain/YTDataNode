package recover

import(
	"sync"
	"time"
)


var (
      RbdTaskLK  sync.Mutex
      SuccRbdLk  sync.Mutex
      FailRbdLk  sync.Mutex
      ShardWkCntLk  sync.Mutex
      FailDcdTaskIDLK  sync.Mutex
      SuccShardLk  sync.Mutex
      FailShardLK  sync.Mutex
	  FailSndShardLk sync.Mutex
	  FailTokenLk   sync.Mutex
	  FailConnLk  sync.Mutex
      ConcurrentShardLk  sync.Mutex
      ConCurrentTaskLK  sync.Mutex
      ConGetShardPoolLK sync.Mutex
      FailLessShardLK  sync.Mutex
      PassJudgeLK      sync.Mutex
      SuccConnLk       sync.Mutex
      SuccTokenLk      sync.Mutex
      ShardForRBDLk    sync.Mutex
      RowRbdSuccLK     sync.Mutex
      ColRbdSuccLk     sync.Mutex
      GlobalRbdSuccLk   sync.Mutex
      SuccPutTokenLK      sync.Mutex
)

func (re *RecoverEngine) IncSuccPutTok(){
	SuccPutTokenLK.Lock()
	defer SuccPutTokenLK.Unlock()
	re.rcvstat.successPutToken++
}


func (re *RecoverEngine) IncGlobalRbdSucc(){
	GlobalRbdSuccLk.Lock()
	defer GlobalRbdSuccLk.Unlock()
	re.rcvstat.globalRebuildSucc++
}

func (re *RecoverEngine) IncColRbdSucc(){
	ColRbdSuccLk.Lock()
	defer ColRbdSuccLk.Unlock()
	re.rcvstat.columnRebuildSucc++
}

func (re *RecoverEngine) IncRowRbdSucc(){
	RowRbdSuccLK.Lock()
	defer RowRbdSuccLK.Unlock()
	re.rcvstat.rowRebuildSucc++
}

func (re *RecoverEngine) IncShardForRbd(){
	ShardForRBDLk.Lock()
	defer ShardForRBDLk.Unlock()
	re.rcvstat.shardforRebuild++
}

func (re *RecoverEngine) IncSuccToken(){
	SuccTokenLk.Lock()
	defer SuccTokenLk.Unlock()
	re.rcvstat.successToken++
}

func (re *RecoverEngine) IncSuccConn(){
	SuccConnLk.Lock()
	defer SuccConnLk.Unlock()
	re.rcvstat.sucessConn++
}

func (re *RecoverEngine) IncPassJudge(){
	PassJudgeLK.Lock()
	defer PassJudgeLK.Unlock()
	re.rcvstat.passJudge++
}

func (re *RecoverEngine) IncFailLessShard(){
	FailLessShardLK.Lock()
	defer FailLessShardLK.Unlock()
	re.rcvstat.failLessShard++
}

func (re *RecoverEngine) GetConShardPass(){
	  for{
	  	    <-time.After(time.Millisecond)
			ConGetShardPoolLK.Lock()
			if len(getShardPool) > 0{
				<-getShardPool
				ConGetShardPoolLK.Unlock()
				break
			}
		  ConGetShardPoolLK.Unlock()
	  }
}

func (re *RecoverEngine) ReturnConShardPass(){
	ConGetShardPoolLK.Lock()
	defer ConGetShardPoolLK.Unlock()
	getShardPool <- 0
}

func (re *RecoverEngine) IncConTask(){
	ConCurrentTaskLK.Lock()
	defer ConCurrentTaskLK.Unlock()
	re.rcvstat.concurrentTask++
}

func (re *RecoverEngine) DecConTask(){
	ConCurrentTaskLK.Lock()
	defer ConCurrentTaskLK.Unlock()
	re.rcvstat.concurrentTask--
}

func (re *RecoverEngine) IncConShard(){
	ConcurrentShardLk.Lock()
	defer ConcurrentShardLk.Unlock()
	re.rcvstat.concurrenGetShard++
}

func (re *RecoverEngine) DecConShard(){
	ConcurrentShardLk.Lock()
	defer ConcurrentShardLk.Unlock()
	re.rcvstat.concurrenGetShard--
}

func (re *RecoverEngine) IncFailConn(){
	FailConnLk.Lock()
	defer FailConnLk.Unlock()
	re.rcvstat.failConn++
}

func (re *RecoverEngine) IncFailToken(){
	FailTokenLk.Lock()
	defer FailTokenLk.Unlock()
	re.rcvstat.failToken++
}

func (re *RecoverEngine) IncFailSendShard(){
	FailSndShardLk.Lock()
	defer FailSndShardLk.Unlock()
	re.rcvstat.failSendShard++
}

func (re *RecoverEngine) IncFailShard(){
	FailShardLK.Lock()
	defer FailShardLK.Unlock()
	re.rcvstat.failShard++
}

func (re *RecoverEngine) IncSuccShard(){
	SuccShardLk.Lock()
	defer SuccShardLk.Unlock()
	re.rcvstat.successShard++
}

func (re *RecoverEngine) IncFailDcdTask(){
	FailDcdTaskIDLK.Lock()
	defer FailDcdTaskIDLK.Unlock()
	re.rcvstat.failDecodeTaskID++
}

func (re *RecoverEngine) IncGetShardWK(){
	ShardWkCntLk.Lock()
	defer ShardWkCntLk.Unlock()
	re.rcvstat.getShardWkCnt++
}

func (re *RecoverEngine) IncRbdTask(){
	RbdTaskLK.Lock()
	defer RbdTaskLK.Unlock()
	re.rcvstat.rebuildTask++
}

func (re *RecoverEngine) IncReportRbdTask(){
	RbdTaskLK.Lock()
	defer RbdTaskLK.Unlock()
	re.rcvstat.reportTask++
}

func (re *RecoverEngine) IncSuccRbd(){
	SuccRbdLk.Lock()
	defer SuccRbdLk.Unlock()
	re.rcvstat.successRebuild++
}

func (re *RecoverEngine) IncFailRbd(){
	FailRbdLk.Lock()
	defer FailRbdLk.Unlock()
	re.rcvstat.failRebuild++
}

func (re *RecoverEngine) IncRbdSucc(n uint16){
	if 1 == n {
		re.IncRowRbdSucc()
	}

	if 2 == n {
		re.IncGlobalRbdSucc()
	}

	if 3 <= n {
		re.IncGlobalRbdSucc()
	}
}