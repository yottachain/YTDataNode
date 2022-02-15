package mq

import (
	"container/list"
	"sync"
)

type BaseMessageQueue struct {
	curr      chan *Msg
	waitQueue MQ
}

func (bmq *BaseMessageQueue) Pop() *Msg {
	var msg = <-bmq.curr

	if m := bmq.waitQueue.Pop(); m != nil {
		select {
		case bmq.curr <- m:
		default:
			bmq.waitQueue.Push(m.Name, m.Payload)
		}
	}

	return msg
}

func (bmq *BaseMessageQueue) Len() int {
	return bmq.waitQueue.Len()
}

func (bmq *BaseMessageQueue) Push(name string, payload interface{}) error {
	select {
	case bmq.curr <- &Msg{
		Name:    name,
		Payload: payload,
	}:
	default:
		return bmq.waitQueue.Push(name, payload)
	}
	return nil
}

func NewBaseMessageQueue(ql int) *BaseMessageQueue {
	bmq := &BaseMessageQueue{
		curr:      make(chan *Msg, ql),
		waitQueue: &SyncList{list.New(), sync.Mutex{}},
	}
	return bmq
}
