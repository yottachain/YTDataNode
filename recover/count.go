package recover

import(
	"sync"
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
)

func (re *RecoverEngine) IncFailConn(){
	FailTokenLk.Lock()
	defer FailTokenLk.Unlock()
	re.failConn++
}

func (re *RecoverEngine) IncFailToken(){
	FailTokenLk.Lock()
	defer FailTokenLk.Unlock()
	re.failToken++
}

func (re *RecoverEngine) IncFailSendShard(){
	FailSndShardLk.Lock()
	defer FailSndShardLk.Unlock()
	re.failSendShard++
}

func (re *RecoverEngine) IncFailShard(){
	FailShardLK.Lock()
	defer FailShardLK.Unlock()
	re.failShard++
}

func (re *RecoverEngine) IncSuccShard(){
	SuccShardLk.Lock()
	defer SuccShardLk.Unlock()
	re.successShard++
}

func (re *RecoverEngine) IncFailDcdTask(){
	FailDcdTaskIDLK.Lock()
	defer FailDcdTaskIDLK.Unlock()
	re.failDecodeTaskID++
}

func (re *RecoverEngine) IncGetShardWK(){
	ShardWkCntLk.Lock()
	defer ShardWkCntLk.Unlock()
	re.getShardWkCnt++
}

func (re *RecoverEngine) IncRbdTask(){
	RbdTaskLK.Lock()
	defer RbdTaskLK.Unlock()
	re.rebuildTask++
}

func (re *RecoverEngine) IncReportRbdTask(){
	RbdTaskLK.Lock()
	defer RbdTaskLK.Unlock()
	re.reportTask++
}

func (re *RecoverEngine) IncSuccRbd(){
	SuccRbdLk.Lock()
	defer SuccRbdLk.Unlock()
	re.successRebuild++
}

func (re *RecoverEngine) IncFailRbd(){
	FailRbdLk.Lock()
	defer FailRbdLk.Unlock()
	re.failRebuild++
}

