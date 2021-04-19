package mq

import (
	"container/list"
	"sync"
)

type SyncList struct {
	*list.List
	sync.Mutex
}

func (s SyncList) Push(name string, payload interface{}) error {
	s.Lock()
	defer s.Unlock()
	s.PushBack(&Msg{
		Name:    name,
		Payload: payload,
	})
	return nil
}

func (s SyncList) Pop() *Msg {
	s.Lock()
	defer s.Unlock()

	if e := s.Front(); e != nil {
		defer s.Remove(e)
		if msg, ok := e.Value.(*Msg); ok {
			return msg
		}
	}

	return nil
}
