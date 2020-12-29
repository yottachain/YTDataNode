package activeNodeList

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

var locker = sync.RWMutex{}

func getUrl() string {
	var url string
	i := rand.Intn(len(config.DefaultConfig.BPList))
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
	Group  byte
}

func Update() {
	url := getUrl()

	res, err := http.Get(url)
	if err != nil {
		log.Println("[activeNodeList]", err.Error())
		return
	}

	dc := json.NewDecoder(res.Body)
	err = dc.Decode(&nodeList)
	if err != nil {
		log.Println("[activeNodeList]", err.Error())
		return
	}

	buf, _ := json.Marshal(nodeList)
	md5Buf := md5.Sum(buf)
	log.Println("[activeNodeList] update success", hex.EncodeToString(md5Buf[:]))
	updateTime = time.Now()
}

func GetNodeList() []Data {
	locker.Lock()
	defer locker.Unlock()

	if time.Now().Sub(updateTime) > time.Minute*30 {
		Update()
	}

	for k, v := range nodeList {
		buf := []byte(v.NodeID)
		nodeList[k].Group = buf[len(buf)-1]
	}

	return nodeList
}
func GetNodeListByGroup(group byte) []Data {
	var res = make([]Data, 0)
	for _, v := range GetNodeList() {
		if v.Group == group {
			res = append(res, v)
		}
	}
	return res
}

func GetGroupList() []byte {
	var m = make(map[byte]int)
	for _, v := range GetNodeList() {
		m[v.Group]++
	}

	var min byte = 255
	var res = make([]byte, 0)
GetMin:
	func(m map[byte]int) {
		for k, _ := range m {
			if k < min {
				min = k
			}
		}
	}(m)
	res = append(res, min)
	delete(m, min)
	min = 255
	if len(m) > 0 {
		goto GetMin
	}
	return res
}

func getYesterdayDuration() time.Duration {
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	yesterday0h, _ := time.ParseInLocation("2006-01-02", yesterday, time.Local)

	return time.Now().Sub(yesterday0h)
}

// 根据时间间隔取node分组
func GetNodeListByTime(duration time.Duration) []Data {
	var groupList = GetGroupList()

	d := getYesterdayDuration()
	index := (d / duration) % time.Duration(len(groupList))
	return GetNodeListByGroup(groupList[index])
}

func HasNodeid(id string) bool {
	locker.Lock()
	defer locker.Unlock()
	if time.Now().Sub(updateTime) > time.Minute*30 {
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
