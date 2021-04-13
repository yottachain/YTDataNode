package recover

import "github.com/yottachain/YTDataNode/statistics"

type RecoverStat struct {
	RebuildTask       uint64 `json:"RebuildTask"`       //下发重建的任务总数
	ConcurrentTask    uint64 `json:"ConcurrentTask"`    //并发进行的重建任务数
	ConcurrenGetShard uint64 `json:"ConcurenGetShard"`  //并发拉取分片数
	SuccessRebuild    uint64 `json:"SuccessRebuild"`    //成功的重建的任务总数
	FailRebuild       uint64 `json:"FailRebuild"`       //重建失败的任务总数
	ReportTask        uint64 `json:"ReportTask"`        //上报的任务总数（包括重建成功和失败的上报）
	GetShardWkCnt     uint64 `json:"getShardWkCnt"`     //拉取分片的总次数
	FailDecodeTaskID  uint64 `json:"failDecodeTaskID"`  //拉取分片时，解码需拉取的分片信息的错误总数（有可能需拉取的分片信息不全）
	Success           uint64 `json:"Success"`           //成功拉取的分片总数
	FailShard         uint64 `json:"FailShard"`         //不存在的总分片数（分片丢失）
	FailSendShard     uint64 `json:"FailSendShard"`     //分片存在，但是传输过程中分片丢失
	FailToken         uint64 `json:"FailToken"`         //拉取分片时，获取不到token的总次数
	FailConn          uint64 `json:"failConn"`          //拉取分片时，无法连接的总数
	FailLessShard     uint64 `json:"failLessShard"`     //在线矿机数不够，无法获取足够分片
	PassJudge         uint64 `json:"passJudge"`         //预判重建成功
	SuccessConn       uint64 `json:"sucessConn"`        //连接成功数
	SuccessToken      uint64 `json:"successToken"`      //获取token成功
	ShardforRebuild   uint64 `json:"shardforRebuild"`   //下载总分片数
	RowRebuildSucc    uint64 `json:"RowRebuildSucc"`    //行方式重建成功
	ColumnRebuildSucc uint64 `json:"ColumnRebuildSucc"` //列方式重建成功
	GlobalRebuildSucc uint64 `json:"GlobalRebuildSucc"` //全局方式重建成功
	RreRebuildSucc    uint64 `json:"RreRebuildSucc"`    //预重建成功
	SuccessPutToken   uint64 `json:"SuccessPutToken"`   //成功释放token总数
	SendTokenReq      uint64 `json:"SendToken"`         //发送token请求计数
	SuccessVersion    uint64 `json:"successVersion"`    //版本验证通过
	AckSuccRebuild    uint64 `json:"AckSuccRebuild"`    //sn确认的成功重建分片数
	RunningCount      *statistics.WaitCount
	DownloadingCount  *statistics.WaitCount
}
