package TaskPool

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type TokenQueue struct {
	Num          int32
	tc           chan *Token
	requestQueue *list.List
	Cancel       context.CancelFunc
	sync.RWMutex
}

type getTokenRequest struct {
	Level int32
	res   chan *Token
}

func newGetTKRequest(level int32) *getTokenRequest {
	return &getTokenRequest{
		Level: level,
		res:   make(chan *Token),
	}
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.tc = make(chan *Token, Num)
	tq.Num = Num
	tq.requestQueue = list.New()
	tq.RWMutex = sync.RWMutex{}
	go tq.Run()
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	tq.Lock()
	defer tq.Unlock()

	request := newGetTKRequest(level)
	if tq.requestQueue.Len() == 0 {
		tq.requestQueue.PushFront(request)
	} else if tq.requestQueue.Len() >= int(tq.Num) {
		request.res <- nil
	}

	walk := tq.requestQueue.Back()
	if request.Level <= walk.Value.(*getTokenRequest).Level {
		tq.requestQueue.PushBack(request)
	} else {
		for {
			if walk.Value.(*getTokenRequest).Level < request.Level {
				if walk.Prev() != nil {
					walk = walk.Prev()
				}
			} else {
				break
			}
		}
		tq.requestQueue.InsertAfter(request, walk)
	}

	return request.res
}

func (tq *TokenQueue) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	tq.Cancel = cancel
	for {
		select {
		case <-ctx.Done():
			return
		case tk := <-tq.tc:
			if tq.requestQueue.Len() > 0 {
				head := tq.requestQueue.Remove(tq.requestQueue.Front())
				if head != nil {
					tk.Reset()
					head.(*getTokenRequest).res <- tk
				}
			} else {
				tq.tc <- tk
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

func (tq *TokenQueue) Add() {
	tk := NewToken()
	tk.Reset()
	select {
	case tq.tc <- tk:
	default:
	}
}
