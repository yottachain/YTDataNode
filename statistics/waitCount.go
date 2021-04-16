package statistics

import (
	"fmt"
	"sync/atomic"
)

type WaitCount struct {
	max  int32
	curr int32
	q    chan struct{}
}

func (wc *WaitCount) Add() {
	if atomic.LoadInt32(&wc.curr) < atomic.LoadInt32(&wc.max) {
		atomic.AddInt32(&wc.curr, 1)
	}
	if atomic.LoadInt32(&wc.curr) >= atomic.LoadInt32(&wc.max) {
		wc.q <- struct{}{}
	}
}

func (wc *WaitCount) Remove() {
	if atomic.LoadInt32(&wc.curr) > 0 {
		atomic.AddInt32(&wc.curr, -1)
		if atomic.LoadInt32(&wc.curr) < atomic.LoadInt32(&wc.max) {
			select {
			case <-wc.q:
			default:
			}
		}
	}
}

func (wc *WaitCount) SetMax(n int32) {
	atomic.StoreInt32(&wc.max, n)
}

func (wc *WaitCount) Len() int {
	return int(atomic.LoadInt32(&wc.curr))
}

func NewWaitCount(max int32) *WaitCount {
	return &WaitCount{
		max:  max,
		curr: 0,
		q:    make(chan struct{}, 1),
	}
}

func (wc *WaitCount) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprint(wc.Len())), nil
}
