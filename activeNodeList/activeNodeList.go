package activeNodeList

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

var locker = sync.RWMutex{}

func getUrl() string {
	var url string
	i := rand.Intn(21)
	if i < 10 {
		url = fmt.Sprintf("http://sn0%d.yottachain.net:8082/readable_nodes", i)
	} else {
		url = fmt.Sprintf("http://sn%d.yottachain.net:8082/readable_nodes", i)
	}
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
	res, err := http.Get(url)
	if err != nil {
		return
	}
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
			return true
		}
	}
	return false
}
