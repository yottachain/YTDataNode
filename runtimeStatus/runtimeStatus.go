package runtimeStatus

import (
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/yottachain/YTDataNode/logger"
)

// RuntimeStatus 运行时状态
type RuntimeStatus struct {
	CPU   []uint32
	AvCPU uint32
	Mem   uint32
}

// NewRuntimeStatus 创建状态管理器
func NewRuntimeStatus() *RuntimeStatus {
	return new(RuntimeStatus)
}

// Update 刷新
func (rs *RuntimeStatus) Update() RuntimeStatus {
	cpupercent, _ := cpu.Percent(0, true)
	var cpupint32 = make([]uint32, len(cpupercent))
	var sumcpu uint32
	for k, v := range cpupercent {
		cpupint32[k] = uint32(v)
		sumcpu = sumcpu + cpupint32[k]
	}
	m, _ := mem.VirtualMemory()
	rs.Mem = uint32(m.UsedPercent)
	rs.CPU = cpupint32
	if uint32(len(cpupercent)) > 0 {
		rs.AvCPU = sumcpu / uint32(len(cpupercent))
	}
	log.Println("cupsum:", sumcpu, len(cpupercent))
	return *rs
}
