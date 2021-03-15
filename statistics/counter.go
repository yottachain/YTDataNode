package statistics

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type MyTime struct {
	time.Time
}

func (mt *MyTime) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	if mt == nil {
		buf.WriteString("")
	} else {
		buf.WriteString(mt.Format("2006-01-02 03:04:05"))
	}
	return buf.Bytes(), nil
}

type RateCounter struct {
	Count      int64
	Success    int64
	clearTime  *MyTime
	UpdateTime *MyTime
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
	rc.UpdateTime = &MyTime{time.Now()}

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
	rc.clearTime = &MyTime{time.Now()}
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
	if time.Now().Sub(rc.clearTime.Time) > time.Minute*10 {
		rc.Reset()
	}
	return rate
}
