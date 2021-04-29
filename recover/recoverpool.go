package recover

import (
	"github.com/yottachain/YTDataNode/statistics"
	"time"
	//"sync"
)

var DownloadCount *statistics.WaitCount
var RunningCount *statistics.WaitCount

var totalCap int32 = 10
var realConCurrent uint16 = 1 //can be changed by write-weight and config
//var realConTask uint16 = 20
var realConTask uint16 = 1

func (re *RecoverEngine) doRequest(task *Task, pkgstart time.Time) {
	re.IncConTask()
	re.descriptionTask(task, pkgstart)
	re.DecConTask()
	RunningCount.Remove()
}

func (re *RecoverEngine) processRequests() {
	startTsk := time.Now()

	for {
		requestT := re.waitQueue.GetTask()
		if requestT == nil {
			continue
		}

		RunningCount.Add()
		re.IncRbdTask()
		go re.doRequest(requestT, startTsk)
	}
}

func (re *RecoverEngine) RunPool() {
	RunningCount = statistics.NewWaitCount(totalCap)
	DownloadCount = statistics.NewWaitCount(totalCap)

	go re.processRequests()

	for {
		re.MultiReply()
	}
}
