package spotCheck

import (
	"fmt"
	"github.com/yottachain/YTDataNode/message"
	"math/rand"
	"testing"
	"time"
)

func TestSpotChecker_Do(t *testing.T) {
	sc := NewSpotChecker()
	// 模拟1000请求 100 毫秒
	sc.TaskList = makeTaskList(1000)
	sc.TaskHandler = handler
	startTime := time.Now()
	sc.Do()
	endTime := time.Now()
	t.Log(sc.progress, sc.InvalidNodeList, len(sc.InvalidNodeList))
	t.Log(endTime.Sub(startTime).String())
}

func makeTaskList(num int) []*message.SpotCheckTask {
	taskList := make([]*message.SpotCheckTask, num)
	i := 0
	for {
		if i >= num {
			break
		}
		taskList[i] = &message.SpotCheckTask{Id: int32(i), NodeId: fmt.Sprintf("node-%d", i)}
		i = i + 1
	}
	return taskList
}

func handler(task *message.SpotCheckTask) bool {
	t := time.Millisecond * time.Duration(rand.Int()%100+100)
	<-time.After(t)
	fmt.Println(task.NodeId, "延时", t.String())

	return rand.Int()%7 != 1
}
