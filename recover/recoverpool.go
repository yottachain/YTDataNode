package recover

import (
	"github.com/yottachain/YTDataNode/statistics"
	"time"
	//"sync"
)

var totalCap int32 = 50
var realConCurrent uint16 = 1 //can be changed by write-weight and config
//var realConTask uint16 = 20
var realConTask uint16 = 1

func (re *Engine) doRequest(task *Task, pkgstart time.Time) {
	statistics.DefaultRebuildCount.IncConTask()
	re.dispatchTask(task, pkgstart)
	statistics.DefaultRebuildCount.DecConTask()
	statistics.RunningCount.Remove()
}

func (re *Engine) processRequests() {
	startTsk := time.Now()

	for {
		requestT := re.waitQueue.GetTask()
		if requestT == nil {
			continue
		}

		statistics.RunningCount.Add()
		statistics.DefaultRebuildCount.IncRbdTask()
		go re.doRequest(requestT, startTsk)
	}
}

func (re *Engine) RunPool() {
	statistics.RunningCount = statistics.NewWaitCount(totalCap)
	statistics.DownloadCount = statistics.NewWaitCount(1000)

	go re.processRequests()

	for {
		re.MultiReply()
	}
}
