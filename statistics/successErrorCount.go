/**
 * @Description: 成功失败计数器
 */
package statistics

import "sync/atomic"

type stat struct {
	Success int32
	Error   int32
}

func (s *stat) AddSuccess() {
	atomic.AddInt32(&s.Success, 1)
}
func (s *stat) AddError() {
	atomic.AddInt32(&s.Error, 1)
}
func (s *stat) Reset() {
	atomic.StoreInt32(&s.Success, 0)
	atomic.StoreInt32(&s.Error, 0)
}
func (s *stat) Get() (int32, int32) {
	return atomic.LoadInt32(&s.Success), atomic.LoadInt32(&s.Error)
}
