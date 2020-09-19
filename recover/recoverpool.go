package recover

import(
	log "github.com/yottachain/YTDataNode/logger"
	"time"
)

var poolG chan int
var totalCap int = 200
var realConCurrent int = 10     //can be changed by write-weight and config

type Request struct {
	 Tsk     *Task
	 Response chan []string    //用于存放请求结果的通道
}

var requestChannelG chan Request

func (re *RecoverEngine) sendRequest() {
	task := <- re.queue
	responseChanT := make(chan []string, 1)
	defer close(responseChanT)
	requestChannelG <- Request{Tsk:task,Response:responseChanT}
}

func (re *RecoverEngine) doRequest(request Request){
    re.processTask(request)
    poolG <- 0
}

func (re *RecoverEngine)processRequests(){
	for {
		requestT := <-requestChannelG
		if len(poolG) > 0 {
			<- poolG
			go re.doRequest(requestT)
		} else {
			requestT.Response <- []string{"goroutine pool is full"}
			<- time.Second
		}
	}
}

func (re *RecoverEngine)RunPool(){
	poolG = make(chan int, totalCap)
	requestChannelG = make(chan Request, 100)
	defer close(requestChannelG)
	defer close(poolG)

	go re.processRequests()

	for i := 0; i < realConCurrent; i++ {
		poolG <- 0
	}

	for i := 0; i < 20; i++ {
		go re.sendRequest()
	}

	log.Println("[recover] use gorutine pool for recover shard")

}