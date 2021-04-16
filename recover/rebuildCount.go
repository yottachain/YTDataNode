package recover

type RebuildCount struct {
	rebuildTask       uint64
	concurrentTask    uint64
	concurrenGetShard uint64
	successRebuild    uint64
	failRebuild       uint64
	reportTask        uint64
	getShardWkCnt     uint64
	failDecodeTaskID  uint64
	successShard      uint64
	failShard         uint64
	failSendShard     uint64
	failToken         uint64
	failConn          uint64
	failLessShard     uint64
	passJudge         uint64
	sucessConn        uint64
	successToken      uint64
	shardforRebuild   uint64
	rowRebuildSucc    uint64
	columnRebuildSucc uint64
	globalRebuildSucc uint64
	preRebuildSucc    uint64
	successPutToken   uint64
	sendTokenReq      uint64
	successVersion    uint64
	ackSuccRebuild    uint64
}
