/**
 * @Description: 成功失败计数器
 */
package statistics

import (
	log "github.com/yottachain/YTDataNode/logger"
	"sync/atomic"
	"time"
)

type SuccessError struct {
	Success int32
	Error   int32
}

func (s *SuccessError) AddSuccess() {
	atomic.AddInt32(&s.Success, 1)
}
func (s *SuccessError) AddError() {
	atomic.AddInt32(&s.Error, 1)
}
func (s *SuccessError) Reset() {
	atomic.StoreInt32(&s.Success, 0)
	atomic.StoreInt32(&s.Error, 0)
}
func (s *SuccessError) Get() (int32, int32) {
	return atomic.LoadInt32(&s.Success), atomic.LoadInt32(&s.Error)
}

type statusCount struct {
	Total    uint64
	Success0 uint64
	Success1 uint64
	Success2 uint64
	Success3 uint64
	Error    uint64
}

var DefaultStatusCount statusCount

func init() {
	go func() {
		for {
			<-time.After(time.Second * 5)
			log.Println("重建状态", DefaultStatusCount)
		}
	}()
}
