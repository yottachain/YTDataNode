package activeNodeList

import (
	"encoding/json"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

var locker = sync.RWMutex{}

func getUrl() string {
	var url string
	var i int
	if len(config.DefaultConfig.BPList) == 1 {
		i = 0
	} else {
		i = rand.Intn(len(config.DefaultConfig.BPList))
	}
	timeRange := config.Gconfig.OutlineTimeRange
	if timeRange == 0 {
		timeRange = 600
	}
	peerInfo := config.DefaultConfig.BPList[i]
	url = fmt.Sprintf("http://%s:8082/readable_nodes", strings.Split(peerInfo.Addrs[0], "/")[2])
	return url
}

var nodeList []Data
var updateTime = time.Time{}

type Data struct {
	NodeID string   `json:"nodeid"`
	ID     string   `json:"id"`
	IP     []string `json:"ip"`
}

func Update() {
	url := getUrl()

	res, err := http.Get(url)
	if err != nil {
		fmt.Println("[recover][nodnlist]can't get active datanode list!")
		return
	}

	dc := json.NewDecoder(res.Body)
	err = dc.Decode(&nodeList)
	if err != nil {
		return
	}
	updateTime = time.Now()
}

func GetNodeList() []Data {
	locker.Lock()
	defer locker.Unlock()

	if time.Now().Sub(updateTime) > time.Minute*30 {
		Update()
	}

	return nodeList
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
