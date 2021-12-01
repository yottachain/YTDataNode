package actuator

import "time"

/**
 * @Description: 重建恢复类型
 */
type RecoverStage uint8

const (
	RECOVER_STAGE_CP RecoverStage = iota
	RECOVER_STAGE_ROW
	RECOVER_STAGE_COL
	RECOVER_STAGE_FULL
)

type Options struct {
	//Expired time.Time    // 超时时间
	Expired int32    // 超时时间
	STime 	time.Time	//开始时间
	Stage   RecoverStage // 重建阶段 0. 行 1. 列 2. 全局
}

/**
 * @Description: 重建任务执行器
 */
type Actuator interface {
	ExecTask([]byte, Options) ([]byte, error)
}
