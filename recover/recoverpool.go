package recover

import (
	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"time"
	//"sync"
)

var getShardPool chan int
var poolG chan int
var totalCap int = 2000
var realConCurrent uint16 = 1 //can be changed by write-weight and config
//var realConTask uint16 = 20
var realConTask uint16 = 1

func (re *RecoverEngine) doRequest(task *Task, pkgstart time.Time) {
	re.IncConTask()
	re.processTask(task, pkgstart)
	re.DecConTask()
	poolG <- 0
}

func (re *RecoverEngine) processRequests() {
	startTsk := time.Now()
	receiveTask := 0
	notexecTask := 0

	for {
		requestT := re.waitQueue.GetTask()
		receiveTask++
		//log.Println("[recover] create_gorutine, recieveTask=",receiveTask,"notexecTask=",notexecTask)

		if 0 == re.startTskTmCtl {
			startTsk = time.Now()
			log.Println("[recover] task_package start_time=", time.Now().Unix(), "len=", re.waitQueue.Len()+1)
			re.startTskTmCtl++
		}

		if re.waitQueue.Len() <= 0 {
			re.startTskTmCtl = 0
		}

		if time.Now().Sub(startTsk).Seconds() > (float64(requestT.TaskLife - 120)) {
			msg := requestT.Data
			if len(msg) > 2 {
				msgData := msg[2:]
				var tsk message.TaskDescription
				proto.Unmarshal(msgData, &tsk)
				if len(tsk.Id) > 8 {
					log.Printf("[recover]time_expired, taskid=%d", BytesToInt64(tsk.Id[0:8]))
				} else {
					log.Println("[recover]time_expired")
				}
			} else {
				log.Println("[recover]time_expired")
			}
			notexecTask++
			continue
		}

		<-poolG
		re.IncRbdTask()
		log.Println("[recover] create_gorutine, realRecoverTask=", re.rcvstat.rebuildTask)
		go re.doRequest(requestT, startTsk)
	}
}

func (re *RecoverEngine) modifyPoolSize() {
	utp := re.Upt

	for {
		<-time.After(time.Second * 600)
		configweight := config.Gconfig.ShardRbdConcurrent
		tokenweight := (time.Second / utp.FillTokenInterval) / 2
		realConCurrent_N := configweight
		realConTask_N := realConCurrent_N * 20
		if uint16(tokenweight) < realConCurrent_N {
			realConCurrent_N = uint16(tokenweight)
			realConTask_N = realConCurrent_N * 20
		}

		if realConCurrent_N > 2000 {
			realConCurrent_N = 2000
			realConTask_N = realConCurrent_N * 20
		}

		if realConCurrent_N == 0 {
			realConCurrent_N = 1
			realConTask_N = 20
		}

		if realConCurrent < realConCurrent_N {
			for k := uint16(0); k < realConCurrent_N-realConCurrent; k++ {
				getShardPool <- 0
			}

			for k := uint16(0); k < realConTask_N-realConTask; k++ {
				poolG <- 0
			}

			realConCurrent = realConCurrent_N
			realConTask = realConTask_N
		}

		if realConCurrent > realConCurrent_N {
			for k := uint16(0); k < realConCurrent-realConCurrent_N; k++ {
				<-getShardPool
			}

			for k := uint16(0); k < realConTask-realConTask_N; k++ {
				<-poolG
			}
			realConCurrent = realConCurrent_N
			realConTask = realConTask_N
		}

		log.Println("[recover] realConCurent=", realConCurrent)
	}
}

func (re *RecoverEngine) RunPool() {
	poolG = make(chan int, totalCap)
	defer close(poolG)

	getShardPool = make(chan int, totalCap)
	defer close(getShardPool)

	go re.processRequests()

	//go re.modifyPoolSize()

	for i := uint16(0); i < realConCurrent; i++ {
		getShardPool <- 0
	}

	for k := uint16(0); k < realConTask; k++ {
		poolG <- 0
	}

	for {
		re.MultiReply()
	}
}
