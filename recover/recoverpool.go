package recover

import (
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/statistics"
	"time"
	//"sync"
)

var totalCap int32 = 50
var realConCurrent uint16 = 1 //can be changed by write-weight and config
//var realConTask uint16 = 20
var realConTask uint16 = 20

func (re *Engine) doRequest(task *Task, pkgstart time.Time, taskPool chan struct{}) {
	statistics.DefaultRebuildCount.IncConTask()
	re.dispatchTask(task, pkgstart)
	statistics.DefaultRebuildCount.DecConTask()
	statistics.RunningCount.Remove()
	taskPool <- struct{}{}
}

func (re *Engine) processRequests(taskPool chan struct{}) {
	var  k  uint64
	var  n  uint64
	var  m  uint64
	for{
		startTsk := time.Now()

		for {
			<- taskPool
			requestT := re.waitQueue.GetTask()
			k++
			if requestT == nil {
				m++
				if m % 10000 == 0{
					log.Println("[recover] k=", k, "m=", m," processRequests is nil")
				}
				//time.Sleep(time.Second)
				//break
				continue
			}
			n++
			statistics.RunningCount.Add()
			statistics.DefaultRebuildCount.IncRbdTask()
			if n % 100 == 0 {
				log.Println("[recover] k= ",k," n=", n, " processRequests:",requestT)
			}
			go re.doRequest(requestT, startTsk, taskPool)
		}
	}
}

func (re *Engine) RunPool() {
	statistics.RunningCount = statistics.NewWaitCount(totalCap)
	statistics.DownloadCount = statistics.NewWaitCount(1000)
	taskPool := make(chan struct{},realConTask)
	for i := uint16(0); i < realConTask; i++{
		taskPool  <- struct {}{}
	}

	go re.processRequests(taskPool)

	for {
		_ = re.MultiReply()
	}
}
