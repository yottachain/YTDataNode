package recover

import "time"

type Task struct {
	SnID        int32
	Data        []byte
	ExpiredTime int64	//下发任务端的过期时间，也就是它的系统时间
	TaskLife    int32	//任务的相对时间单位秒
	SrcNodeID   int32
	StartTime 	time.Time	//收到任务的时间
	ExecTimes   uint	//任务被执行过多少次， 超过一定次数任务丢弃
	Type		int32	
}