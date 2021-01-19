package TaskPool

import (
	"container/list"
	"sync"
	"time"
)

type TokenQueue struct {
	tc           *chan *Token
	requestQueue *list.List
	Max          int32
	sync.RWMutex
}

func (tq *TokenQueue) Len() int {
	return len(*tq.tc)
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.Max = Num
	tc := make(chan *Token, Num)
	tq.tc = &tc
	tq.requestQueue = list.New()
	go tq.Run()
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	req := NewRequest(level)

	tq.Lock()
	defer tq.Unlock()
	backE := tq.requestQueue.Back()
	if backE != nil && backE.Value.(*request).Level < req.Level {
		tq.requestQueue.PushFront(req)
	} else {
		tq.requestQueue.PushBack(req)
	}
	return req.Res
}

func (tq *TokenQueue) Run() {
	for {
		select {
		case tk := <-*tq.tc:
			tq.RLock()
			if tq.requestQueue.Len() > 0 {
				reqE := tq.requestQueue.Remove(tq.requestQueue.Front())
				if reqE != nil {
					req := reqE.(*request)
					select {
					case req.Res <- tk:
					default:
					}
				}
			}
			tq.RUnlock()
		case <-time.After(time.Minute):
			tq.Reset()
		}
	}
}

func (tq *TokenQueue) Reset() {
	//log.Println("[token queue]reset")
	tc := make(chan *Token, tq.Max)
	tq.tc = &tc
	tq.requestQueue = list.New()
}

func (tq *TokenQueue) Add() {
	tk := NewToken()
	select {
	case *tq.tc <- tk:
	default:
	}
}
