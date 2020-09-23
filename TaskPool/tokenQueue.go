package TaskPool

import (
	"container/list"
	"sync"
)

type TokenQueue struct {
	tc           chan *Token
	requestQueue *list.List
	sync.RWMutex
}

type request struct {
	Level int32
	Res   chan *Token
}

func NewRequest(level int32) *request {
	var req = new(request)
	req.Level = level
	req.Res = make(chan *Token)
	return req
}

func (tq *TokenQueue) Len() int {
	return len(tq.tc)
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.tc = make(chan *Token, Num)
	tq.requestQueue = list.New()
	//go tq.Run()
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	//req := NewRequest(level)
	//
	//tq.Lock()
	//defer tq.Unlock()
	//backE := tq.requestQueue.Back()
	//if backE != nil && backE.Value.(*request).Level > req.Level {
	//	req.Res <- nil
	//}
	//tq.requestQueue.PushBack(req)
	return tq.tc
}

func (tq *TokenQueue) Run() {
	for {
		tk := <-tq.tc
		tq.RLock()
		if tq.requestQueue.Len() > 0 {
			reqE := tq.requestQueue.Remove(tq.requestQueue.Front())
			if reqE != nil {
				req := reqE.(*request)
				req.Res <- tk
			}
		}
		tq.RUnlock()
	}
}

func (tq *TokenQueue) Add() {
	tk := NewToken()
	tq.tc <- tk
}
