package statistics

import (
	"math"
	"time"

	log "github.com/yottachain/YTDataNode/logger"
)

const SHARD_NUMS = 164

var PfStatChan = make(chan *PerformanceStat, 2000)

type RecoverStat struct {
	RebuildTask        uint64 `json:"RebuildTask"`       //下发重建的任务总数
	ConcurrentTask     uint64 `json:"ConcurrentTask"`    //并发进行的重建任务数
	ConcurrenGetShard  uint64 `json:"ConcurenGetShard"`  //并发拉取分片数
	SuccessRebuild     uint64 `json:"SuccessRebuild"`    //成功的重建的任务总数
	FailRebuild        uint64 `json:"FailRebuild"`       //重建失败的任务总数
	ReportTask         uint64 `json:"ReportTask"`        //上报的任务总数（包括重建成功和失败的上报）
	GetShardWkCnt      uint64 `json:"getShardWkCnt"`     //拉取分片的总次数
	FailDecodeTaskID   uint64 `json:"failDecodeTaskID"`  //拉取分片时，解码需拉取的分片信息的错误总数（有可能需拉取的分片信息不全）
	Success            uint64 `json:"Success"`           //成功拉取的分片总数
	FailShard          uint64 `json:"FailShard"`         //不存在的总分片数（分片丢失）
	FailSendShard      uint64 `json:"FailSendShard"`     //分片存在，但是传输过程中分片丢失
	FailToken          uint64 `json:"FailToken"`         //拉取分片时，获取不到token的总次数
	FailConn           uint64 `json:"failConn"`          //拉取分片时，无法连接的总数
	FailLessShard      uint64 `json:"failLessShard"`     //在线矿机数不够，无法获取足够分片
	PassJudge          uint64 `json:"passJudge"`         //预判重建成功
	SuccessConn        uint64 `json:"sucessConn"`        //连接成功数
	SuccessToken       uint64 `json:"successToken"`      //获取token成功
	ShardforRebuild    uint64 `json:"shardforRebuild"`   //下载总分片数
	BackupRebuildSucc  uint64 `json:"BackupRebuildSucc"` //lrc2备份重建成功
	RowRebuildSucc     uint64 `json:"RowRebuildSucc"`    //行方式重建成功
	ColumnRebuildSucc  uint64 `json:"ColumnRebuildSucc"` //列方式重建成功
	GlobalRebuildSucc  uint64 `json:"GlobalRebuildSucc"` //全局方式重建成功
	RreRebuildSucc     uint64 `json:"RreRebuildSucc"`    //预重建成功
	SuccessPutToken    uint64 `json:"SuccessPutToken"`   //成功释放token总数
	SendTokenReq       uint64 `json:"SendToken"`         //发送token请求计数
	SuccessVersion     uint64 `json:"successVersion"`    //版本验证通过
	AckSuccRebuild     uint64 `json:"AckSuccRebuild"`    //sn确认的成功重建分片数
	RunningCount       uint64
	DownloadingCount   uint64
	RowRebuildCount    uint64
	ColRebuildCount    uint64
	GlobalRebuildCount uint64
	NotAllExist        uint64
}

type ShardPerformanceStat struct {
	Hash        string
	DlSuc       bool //是否下载成功
	DlTimes     int  //下载的次数
	DlStartTime time.Time
	DlUseTime   int64 //下载用时
}

type PerformanceStat struct {
	TaskId    uint64
	ExecTimes int64
	DlShards  [SHARD_NUMS]*ShardPerformanceStat
}

func (pf *PerformanceStat) TaskPfStat() {
	if pf == nil {
		return
	}

	var dlShards = 0
	var dlTotalTime = int64(0)
	var dlShardTotalTimes = 0
	var dlTimePerShard = int64(0)
	var dlMaxUseTime = int64(0)
	var variance = float64(0)
	var powSum = float64(0)
	var dlSucRate = float64(0)
	var dlNumsPerShard = float64(0)

	//求平均每分片下载时间
	for _, shard := range pf.DlShards {
		if shard == nil {
			continue
		}

		if shard.DlSuc == true {
			dlShards++
			dlTotalTime += shard.DlUseTime
			dlShardTotalTimes += shard.DlTimes
			if shard.DlUseTime > dlMaxUseTime {
				dlMaxUseTime = shard.DlUseTime
			}
			log.Printf("[recover] task=%d, shard=%s, download_use_time=%d, download_times=%d\n",
				pf.TaskId, shard.Hash, shard.DlUseTime, shard.DlTimes)
		}
	}

	if dlShards != 0 {
		dlTimePerShard = dlTotalTime / int64(dlShards)
	}

	//求方差
	if dlTimePerShard != 0 {
		for _, shard := range pf.DlShards {
			if shard == nil {
				continue
			}

			if shard.DlSuc == true {
				pow := math.Pow(math.Abs(float64(shard.DlUseTime-dlTimePerShard)), 2)
				log.Printf("[recover] task=%d, shard=%s, download_use_time=%d, download_avg_time=%d pow=%.2f\n",
					pf.TaskId, shard.Hash, shard.DlUseTime, dlTimePerShard, pow)
				powSum += pow
			}
		}
	}

	if dlShards != 0 {
		variance = powSum / float64(dlShards)
		dlNumsPerShard = float64(dlShardTotalTimes) / float64(dlShards)
	}

	if dlShardTotalTimes != 0 {
		dlSucRate = float64(dlShards) / float64(dlShardTotalTimes)
	}

	log.Printf("[recover] pf_stat task=%d exec_time=%d download_shard_nums=%d"+
		" max_download_time_of_all_shard=%d avg_download_time_of_per_shard=%d variance_is=%.2f"+
		" suc_rate_of_download_per_shard=%.2f, avg_download_times_of_per_shard=%.2f\n",
		pf.TaskId, pf.ExecTimes, dlShards, dlMaxUseTime, dlTimePerShard,
		math.Sqrt(variance), dlSucRate, dlNumsPerShard)
}

func TimerStatTaskPf() {
	for {
		if len(PfStatChan) >= 1000 {
			var timePerTask = int64(0)
			var allTaskTotalTime = int64(0)
			var powSumTask = float64(0)
			var taskVariance = float64(0)

			var arrPfstat []*PerformanceStat

			for i := 0; i < 1000; i++ {
				pfstat := <-PfStatChan
				allTaskTotalTime += pfstat.ExecTimes
				arrPfstat = append(arrPfstat, pfstat)
			}

			timePerTask = allTaskTotalTime / 1000

			for _, pf := range arrPfstat {
				powSumTask += math.Pow(math.Abs(float64(pf.ExecTimes-timePerTask)), 2)
			}

			taskVariance = powSumTask / 1000

			log.Printf("[recover] task_nums=%d, avg_exec_time_of_per_task=%d, variance_is=%.2f\n",
				1000, timePerTask, math.Sqrt(taskVariance))

			var dlShards = 0
			var dlTotalTime = int64(0)
			var dlShardTotalTimes = 0
			var dlTimePerShard = int64(0)
			var dlMaxUseTime = int64(0)
			var powSumShard = float64(0)
			var variance = float64(0)
			var dlSucRate = float64(0)
			var dlNumsPerShard = float64(0)

			for _, taskPf := range arrPfstat {
				//求平均每分片下载时间
				for _, shard := range taskPf.DlShards {
					if shard == nil {
						continue
					}

					if shard.DlSuc == true {
						dlShards++
						dlTotalTime += shard.DlUseTime
						dlShardTotalTimes += shard.DlTimes
						if shard.DlUseTime > dlMaxUseTime {
							dlMaxUseTime = shard.DlUseTime
						}
					}
				}
			}

			if dlShards != 0 {
				dlTimePerShard = dlTotalTime / int64(dlShards)
			}

			//求方差
			if dlTimePerShard != 0 {
				for _, taskPf := range arrPfstat {
					for _, shard := range taskPf.DlShards {
						if shard == nil {
							continue
						}

						if shard.DlSuc == true {
							powSumShard += math.Pow(math.Abs(float64(shard.DlUseTime-dlTimePerShard)), 2)
						}
					}
				}
			}

			if dlShards != 0 {
				variance = powSumShard / float64(dlShards)
				dlNumsPerShard = float64(dlShardTotalTimes) / float64(dlShards)
			}

			if dlShardTotalTimes != 0 {
				dlSucRate = float64(dlShards) / float64(dlShardTotalTimes)
			}

			log.Printf("[recover] task_nums=%d total_download_shards_is=%d "+
				" max_download_time_of_all_shard=%d avg_download_time_of_per_shard=%d"+
				" variance_is=%.2f suc_rate_of_download_per_shard=%.2f avg_download_times_of_per_shard=%.2f\n",
				1000, dlShards, dlMaxUseTime, dlTimePerShard, math.Sqrt(variance), dlSucRate, dlNumsPerShard)
		}

		<-time.After(time.Second * 1)
	}
}
