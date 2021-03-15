package statistics

import (
	"sync"
	"sync/atomic"
	"time"
)

type RateCounter struct {
	Count      int64
	Success    int64
	clearTime  time.Time
	UpdateTime string
	sync.Mutex
}

func (rc *RateCounter) AddCount() {
	rc.Lock()
	defer rc.Unlock()

	rc.Count++
}
func (rc *RateCounter) AddSuccess() {
	rc.Lock()
	defer rc.Unlock()
	rc.UpdateTime = time.Now().Format("2016-01-02 03:04:05")

	count := rc.Count
	success := rc.Success
	if success >= count {
		rc.Count++
	}
	rc.Success++
}
func (rc *RateCounter) Reset() {
	rc.Lock()
	defer rc.Unlock()

	rc.Count = 0
	rc.Success = 0
	rc.clearTime = time.Now()
}

func (rc *RateCounter) GetRate() int64 {
	if rc == nil {
		return -1
	}
	count := atomic.LoadInt64(&rc.Count)
	success := atomic.LoadInt64(&rc.Success) * 100

	if count == 0 {
		return -1
	}
	rate := success / count
	if time.Now().Sub(rc.clearTime) > time.Minute*10 {
		rc.Reset()
	}
	return rate
}
