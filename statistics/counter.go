package statistics

import (
	"fmt"
	"sync/atomic"
	"time"
)

type RateCounter struct {
	Count     int64
	Success   int64
	clearTime time.Time
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
	rc.clearTime = time.Now()
}

func (rc *RateCounter) GetRate() int64 {
	if rc == nil {
		return 80
	}
	count := atomic.LoadInt64(&rc.Count)
	success := atomic.LoadInt64(&rc.Success) * 100
	if count == 0 {
		return 80
	}
	rate := success / count
	//if time.Now().Sub(rc.clearTime) > time.Minute*10 {
	//	rc.Reset()
	//}
	return rate
}
func (rc *RateCounter) Marshal(v interface{}) ([]byte, error) {
	return []byte(fmt.Sprintf("{\"Count\":%d,\"Success\":%d,\"Rate\":%d}", rc.Count, rc.Success, rc.GetRate())), nil
}
