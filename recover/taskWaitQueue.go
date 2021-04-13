package recover

import "github.com/yottachain/YTDataNode/mq"

type TaskWaitQueue struct {
	*mq.BaseMessageQueue
}

func (twq *TaskWaitQueue) PutTask(task []byte, snid int32, expried int64, srcNodeId int32, tasklife int32) error {
	t := &Task{
		SnID:        snid,
		Data:        task,
		ExpriedTime: expried,
		TaskLife:    tasklife,
		SrcNodeID:   srcNodeId,
	}
	return twq.Push("new_rebuild_task", t)
}

func (twq *TaskWaitQueue) GetTask() *Task {
	return twq.Pop().Payload.(*Task)
}

func NewTaskWaitQueue() *TaskWaitQueue {
	return &TaskWaitQueue{mq.NewBaseMessageQueue(1)}
}
