package spotCheck

import (
	"github.com/yottachain/YTDataNode/message"
	"sync"
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
		sc.InvalidNodeList = append(sc.InvalidNodeList, task.Id)
	}
}
