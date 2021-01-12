package statistics

import (
	"sync/atomic"
	"time"
)

type RateCounter struct {
	Count     int64
	Success   int64
	ClearTime time.Time
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
	rc.ClearTime = time.Now()
}

func (rc *RateCounter) GetRate() int64 {
	if rc == nil {
		return 100
	}
	defer func() {
		if time.Now().Sub(rc.ClearTime) > time.Minute*10 {
			rc.Reset()
		}
	}()
	count := atomic.LoadInt64(&rc.Count)
	success := atomic.LoadInt64(&rc.Success) * 100
	if count == 0 {
		return 0
	}
	rate := success / count
	return rate
}
