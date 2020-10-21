package recover

import (
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"time"
	//"sync"
)

var getShardPool chan int
var poolG chan int
var totalCap int = 2000
var realConCurrent uint16 = 1     //can be changed by write-weight and config
var realConTask uint16 = 1

func (re *RecoverEngine) doRequest(task *Task){
    re.IncConTask()
    re.processTask(task)
	re.DecConTask()
    poolG <- 0
}

func (re *RecoverEngine)processRequests(){
	for {
		if len(poolG) > 0 {
			<- poolG
			requestT :=<- re.queue
            re.IncRbdTask()
            log.Println("[recover] create_gorutine, len_poolG=",len(poolG))
			go re.doRequest(requestT)
		} else {
			log.Println("[recover] create_gorutine pool is full, len_poolG=",len(poolG))
			//requestT.Response <- []string{"goroutine pool is full"}
			<- time.After(time.Second * 3)
		}
	}
}

func (re *RecoverEngine)modifyPoolSize(){
	utp := re.Upt
	//configweight := re.sn.Config().ShardRbdConcurrent

    for{
    	<-time.After(time.Second * 600)
		configweight := config.Gconfig.ShardRbdConcurrent
		tokenweight := (time.Second/utp.FillTokenInterval)/2
        realConCurrent_N := configweight
        realConTask_N := realConCurrent_N * 10
        if uint16(tokenweight) < realConCurrent_N {
           realConCurrent_N = uint16(tokenweight)
           realConTask_N = realConCurrent_N * 10
		}

        if realConCurrent_N > 2000 {
        	realConCurrent_N = 2000
        	realConTask_N = realConCurrent_N * 10
		}

		if realConCurrent_N == 0 {
			realConCurrent_N = 1
			realConTask_N = 10
		}

        if realConCurrent < realConCurrent_N {
        	for k := uint16(0); k < realConCurrent_N - realConCurrent; k++{
				getShardPool <- 0
			}

			for k := uint16(0); k < realConTask_N - realConTask; k++{
				poolG <- 0
			}

			realConCurrent = realConCurrent_N
			realConTask = realConTask_N
		}

		if realConCurrent > realConCurrent_N {
			for k := uint16(0); k < realConCurrent - realConCurrent_N; k++{
				<- getShardPool
			}

			for k := uint16(0); k < realConTask - realConTask_N; k++{
				<- poolG
			}
			realConCurrent = realConCurrent_N
			realConTask = realConTask_N
		}

		log.Println("[recover] realConCurent=",realConCurrent)
	}
}


func (re *RecoverEngine)RunPool(){
	poolG = make(chan int, totalCap)
	defer close(poolG)

	getShardPool = make(chan int, totalCap)
	defer close(getShardPool)
	//requestChannelG = make(chan Request, 100)
	//defer close(requestChannelG)

	go re.processRequests()

	//go re.modifyPoolSize()

	for i := uint16(0); i < realConCurrent; i++ {
		getShardPool <- 0
	}

	for k := uint16(0); k < realConTask; k++{
		poolG <- 0
	}

	for {
		re.MultiReply()
	}
}

