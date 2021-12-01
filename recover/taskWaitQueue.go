package recover

import (
	"fmt"
	"github.com/yottachain/YTDataNode/mq"
	"log"
	"time"
)

type TaskWaitQueue struct {
	*mq.BaseMessageQueue
	Max int
}

func (twq *TaskWaitQueue) PutTask(task []byte, snid int32, expried int64,
		srcNodeId int32, tasklife int32, start time.Time) error {
	t := &Task{
		SnID:        snid,
		Data:        task,
		ExpriedTime: expried,
		TaskLife:    tasklife,
		SrcNodeID:   srcNodeId,
		StartTime:   start,
	}

	log.Printf("[recover] task ExpriedTime is %d, taskLife is %d\n", expried, tasklife)

	if twq.BaseMessageQueue.Len() > twq.Max {
		return fmt.Errorf("task queue full")
	}
	return twq.Push("new_rebuild_task", t)
}

func (twq *TaskWaitQueue) GetTask() *Task {
	// 之后替换成硬盘队列
	if tsk := twq.Pop().Payload; tsk != nil {
		res, ok := tsk.(*Task)
		if !ok {
			return nil
		}
		return res
	}
	return nil
}

func NewTaskWaitQueue() *TaskWaitQueue {
	return &TaskWaitQueue{mq.NewBaseMessageQueue(1), 10000}
}