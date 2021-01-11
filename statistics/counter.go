package statistics

import "sync/atomic"

type RateCounter struct {
	Count   int64
	Success int64
}

func (rc *RateCounter) AddCount() {
	atomic.AddInt64(&rc.Count, 1)
}
func (rc *RateCounter) AddSuccess() {
	atomic.AddInt64(&rc.Success, 1)
}
func (rc *RateCounter) Reset() {
	atomic.StoreInt64(&rc.Count, 0)
	atomic.StoreInt64(&rc.Success, 0)
}
