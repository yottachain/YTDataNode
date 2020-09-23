package TaskPool

import (
	"sync"
	"sync/atomic"
	"time"
)

type TokenQueue struct {
	maxTokenNum int32
	sentToken   int32
	preGetTime  time.Time
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
	return int(tq.maxTokenNum - atomic.LoadInt32(&tq.sentToken))
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.maxTokenNum = Num
	tq.preGetTime = time.Now()
	return tq
}

func (tq *TokenQueue) Get(level int32, interval time.Duration) *Token {
	tq.Lock()
	defer tq.Unlock()

	if level == 0 && time.Now().Sub(tq.preGetTime) < interval {
		return nil
	}
	return NewToken()
}
