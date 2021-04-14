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
	// 之后替换成硬盘队列
	if tsk := twq.Pop().Payload; tsk != nil {
		return tsk.(*Task)
	}
	return nil
}

func NewTaskWaitQueue() *TaskWaitQueue {
	return &TaskWaitQueue{mq.NewBaseMessageQueue(1)}
}
