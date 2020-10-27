package activeNodeList

import (
	"encoding/json"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

var locker = sync.RWMutex{}
var testconfig = 1

func getUrl() string {
	var url string
	i := rand.Intn(21)
	timeRange := config.Gconfig.OutlineTimeRange
	if timeRange == 0 {
		timeRange = 600
	}
	if i < 10 {
		url = fmt.Sprintf("http://sn0%d.yottachain.net:8082/readable_nodes?timerange=%d", i, timeRange)
	} else {
		url = fmt.Sprintf("http://sn%d.yottachain.net:8082/readable_nodes?timerange=%d", i, timeRange)
	}
	return url
}

func getUrl1() string {
	var url string
	url = fmt.Sprint("http://139.155.92.246:8082/readable_nodes")

	return url
}

func getUrl2() string {
	var url string
	url = fmt.Sprint("http://172.17.0.4:8082/readable_nodes")

	return url
}

var nodeList []Data
var updateTime = time.Time{}

type Data struct {
	NodeID string `json:"nodeid"`
	ID     string `json:"id"`
}

func Update() {
	url := getUrl()
	//if 1 == testconfig{
	//	url = getUrl1()
	//}
	//
	//if 2 == testconfig{
	//	url = getUrl2()
	//}

	res, err := http.Get(url)
	if err != nil {
			fmt.Println("[recover][nodnlist]can't get active datanode list!")
		return
	}
	//url := getUrl2()
	//res, err := http.Get(url)
	//if err != nil{
	//	//log.Println("[recover][nodnlist]can't get active datanode list!")
	//	return
	//}

	dc := json.NewDecoder(res.Body)
	err = dc.Decode(&nodeList)
	if err != nil {
		return
	}
	updateTime = time.Now()
}

func HasNodeid(id string) bool {
	locker.Lock()
	defer locker.Unlock()
	if time.Now().Sub(updateTime) > time.Minute {
		Update()
	}

	for _, v := range nodeList {
		if v.NodeID == id {
			//log.Println("[recover]find shard:",id)
			return true
		}
	}
	return false
}
