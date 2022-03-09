package recover

import (
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/statistics"
	"time"

	//"sync"
)

var totalCap = 50
var realConCurrent uint16 = 1 //can be changed by write-weight and config

func (re *Engine) doRequest(task *Task) {
	statistics.DefaultRebuildCount.IncConTask()
	re.dispatchTask(task)
	statistics.DefaultRebuildCount.DecConTask()
	statistics.RunningCount.Remove()
}

func (re *Engine) processRequests() {
	var  k  uint64
	var  n  uint64
	var  m  uint64
	for{
		for {
			requestT := re.waitQueue.GetTask()
			k++
			if requestT == nil {
				m++
				if m % 10000 == 0 {
					log.Println("[recover] k=", k, "m=", m," processRequests is nil")
				}
				time.Sleep(time.Millisecond*10)
				continue
			}
			n++
			statistics.RunningCount.Add()
			statistics.DefaultRebuildCount.IncRbdTask()
			if n % 100 == 0 {
				log.Println("[recover] k= ", k, " n=", n)
			}
			go re.doRequest(requestT)
		}
	}
}

func (re *Engine) RunPool() {
	if config.Gconfig.RebuildMaxCc > 0 {
		totalCap = config.Gconfig.RebuildMaxCc
		log.Printf("[recover] max concurrent is %d\n", totalCap)
	}
	statistics.RunningCount = statistics.NewWaitCount(int32(totalCap))
	statistics.DownloadCount = statistics.NewWaitCount(1000)

	go re.processRequests()

	for {
		_ = re.MultiReply()
	}
}
