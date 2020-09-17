package TaskPool

import (
	"container/list"
	"time"
)

type TokenQueue struct {
	Num          int32
	tc           chan *Token
	requestQueue *list.List
}

type getTokenRequest struct {
	Level int32
	res   chan *Token
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.tc = make(chan *Token, Num)
	tq.Num = Num
	tq.requestQueue = list.New()
	tq.Run()
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	gtq := new(getTokenRequest)
	gtq.Level = level
	gtq.res = make(chan *Token)
	if tq.requestQueue.Len() >= int(tq.Num) {
		gtq.res <- nil
		return gtq.res
	}
	walk := tq.requestQueue.Front()
	for {
		if walk.Value.(*getTokenRequest).Level < gtq.Level {
			break
		}
		if walk.Next() != nil {
			walk = walk.Next()
		}
	}

	tq.requestQueue.InsertBefore(gtq, walk)
	return gtq.res
}

func (tq *TokenQueue) Run() {
	for {
		if tq.requestQueue.Len() > 0 {
			tk := <-tq.tc
			gtq := tq.requestQueue.Remove(tq.requestQueue.Front())
			gtq.(*getTokenRequest).res <- tk
		}
		time.Sleep(time.Second)
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
