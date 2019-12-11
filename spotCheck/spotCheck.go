package spotCheck

import (
	"github.com/mr-tron/base58"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"sync"
	"time"
)

type TaskHandler func(task *message.SpotCheckTask) bool

type SpotChecker struct {
	sync.Mutex
	TaskList        []*message.SpotCheckTask
	InvalidNodeList []int32
	progress        int
	TaskHandler     TaskHandler
}

func NewSpotChecker() *SpotChecker {
	sc := new(SpotChecker)
	return sc
}

func (sc *SpotChecker) Do() {
	workers := make(chan int, 10)
	wg := sync.WaitGroup{}
	wg.Add(len(sc.TaskList))
	for _, task := range sc.TaskList {
		workers <- 1
		go func(tk *message.SpotCheckTask) {
			defer func() {
				<-workers
				wg.Done()
			}()
			sc.check(tk)
		}(task)
	}
	wg.Wait()
}

func (sc *SpotChecker) check(task *message.SpotCheckTask) {
	if sc.TaskHandler == nil {
		return
	}
	defer sc.Unlock()
	result := sc.TaskHandler(task)
	sc.Lock()
	sc.progress = sc.progress + 1
	if result != true {
		for i := 0; i < 5; i++ {
			log.Println("抽查重试第:", i, "次", base58.Encode(task.VHF))
			<-time.After(500 * time.Millisecond)
			if sc.TaskHandler(task) {
				return
			}
		}
		sc.InvalidNodeList = append(sc.InvalidNodeList, task.Id)
	}
}
