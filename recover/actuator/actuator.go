package actuator

import "time"

type Options struct {
	Expired time.Time // 超时时间
	Stage   int       // 重建阶段 0. 行 1. 列 2. 全局
}

/**
 * @Description: 重建任务执行器
 */
type Actuator interface {
	ExecTask([]byte, Options) error
}
