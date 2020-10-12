package activeNodeList

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

var locker = sync.RWMutex{}

var url = "http://39.105.184.162:8082/readable_nodes"

var nodeList []Data

type Data struct {
	NodeID string `json:"nodeid"`
	ID     string `json:"id"`
}

func Update() {
	res, err := http.Get(url)
	if err != nil {
		return
	}
	dc := json.NewDecoder(res.Body)
	locker.Lock()
	defer locker.Unlock()
	err = dc.Decode(&nodeList)
	if err != nil {
		return
	}
}

func HasNodeid(id string) bool {
	locker.RLock()
	defer locker.RUnlock()
	for _, v := range nodeList {
		if v.NodeID == id {
			return true
		}
	}
	return false
}

func AutoUpdate(interval time.Duration) {
	go func() {
		for {
			<-time.After(interval)
			Update()
		}
	}()
}
