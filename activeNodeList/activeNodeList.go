package activeNodeList

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"math"
	"net/http"
	"sync"
	"time"
)

var locker = sync.RWMutex{}

func getUrl() string {
	var url = "https://yottachain-sn-intf-cache.oss-cn-beijing.aliyuncs.com/readable_nodes"

	if config.Gconfig.ActiveNodeUrl != nil && len(config.Gconfig.ActiveNodeUrl) > 0 {
		if config.Gconfig.ActiveNodeUrl[0] != "" {
			url = config.Gconfig.ActiveNodeUrl[0]
		}
	}

	if config.IsDev > 0 {
		if config.Gconfig.ActiveNodeUrl != nil && len(config.Gconfig.ActiveNodeUrl) >= config.IsDev {
			if config.Gconfig.ActiveNodeUrl[config.IsDev-1] != "" {
				url = config.Gconfig.ActiveNodeUrl[config.IsDev-1]
			}
		}
	}

	fmt.Printf("config_IsDev=%d, current active node url is %s\n", config.IsDev, url)

	//if config.IsDev == 1 {
	//	url = "http://192.168.1.206:8080/readable_nodes"
	//} else if config.IsDev == 2 {
	//	url = "http://192.168.1.146:8080/readable_nodes"
	//}else if config.IsDev == 3 {
	//	//url = "https://yottachain-sn-dy-cache.oss-cn-beijing.aliyuncs.com/readable_nodes"
	//	url = "https://yottachain-sn-dy-cache.oss-cn-beijing.aliyuncs.com/readable_nodes"
	//}
	return url
}

var nodeList []*Data
var updateTime = time.Time{}

type Data struct {
	NodeID string   `json:"nodeid"`
	ID     string   `json:"id"`
	IP     []string `json:"ip"`
	Weight string   `json:"weight"`
	WInt   int      `json:"TXTokenFillRate"`
	Group  byte
}

func Update() {
	url := getUrl()

	fmt.Printf("[activeNodeList] current active node url is %s\n", url)

	res, err := http.Get(url)
	if err != nil {
		log.Println("[activeNodeList] geturl error:", err.Error())
		return
	}

	defer res.Body.Close()

	dc := json.NewDecoder(res.Body)
	err = dc.Decode(&nodeList)
	if err != nil {
		log.Println("[activeNodeList] Decode error:", err.Error())
		return
	}

	buf, _ := json.Marshal(nodeList)
	md5Buf := md5.Sum(buf)
	log.Println("[activeNodeList] update success ",
		"list len=", len(nodeList), "hash=", hex.EncodeToString(md5Buf[:]))
	updateTime = time.Now()
}

func GetNodeList() []*Data {
	ttl := 300
	if config.Gconfig.ActiveNodeTTL > 0 {
		ttl = config.Gconfig.ActiveNodeTTL
	}
	locker.Lock()
	if time.Now().Sub(updateTime) > time.Duration(ttl)*time.Second {
		Update()
		log.Printf("[activeNodeList] update interval is %d\n", ttl)
	}
	locker.Unlock()

	for k, v := range nodeList {
		buf := []byte(v.NodeID)
		nodeList[k].Group = buf[len(buf)-1]
	}

	log.Println("[activeNodeList] list len=",len(nodeList))

	return nodeList
}
func GetNodeListByGroup(group byte) []*Data {
	nodeList := GetNodeList()
	var res = make([]*Data, 0)
	for k, _ := range nodeList {
		if nodeList[k].Group == group {
			res = append(res, nodeList[k])
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
func GetNodeListByTime(duration time.Duration) []*Data {
	var groupList = GetGroupList()

	d := getYesterdayDuration()
	index := (d / duration) % time.Duration(len(groupList))
	return GetNodeListByGroup(groupList[index])
}

func GetNodeListByTimeAndGroupSize(duration time.Duration, size int) []*Data {

	var groupList = GetGroupList()
	var lg = len(groupList)
	var res = make([]*Data, 0)

	if lg == 0 || duration == 0 {
		return nil
	}

	d := getYesterdayDuration()
	index := (d / duration) % time.Duration(lg)
	for i := 0; i < size; i++ {
		if int(index) >= lg {
			index = 0
		}
		res = append(res, GetNodeListByGroup(groupList[index])...)
		index++
	}
	return res
}

// 获取经过权重处理的NodeList
func GetWeightNodeList(nodeList []*Data) []*Data {
	var res = make([]*Data, 0)
	for k, v := range nodeList {
		nodeList[k].WInt = int(math.Log(float64(v.WInt))) + int(math.Pow(float64(v.WInt/100), 2))
		for i := 0; i <= nodeList[k].WInt; i++ {
			res = append(res, nodeList[k])
		}
	}
	return res
}

func GetNoIPNodeList(data []*Data, ip string) []*Data {
	if ip == "" {
		return data
	}
	return Filter(data, NewNoAddrFilter(ip))
}

func HasNodeid(id string) bool {
	ttl := 300
	if config.Gconfig.ActiveNodeTTL > 0 {
		ttl = config.Gconfig.ActiveNodeTTL
	}
	if time.Now().Sub(updateTime).Seconds() > time.Duration(ttl).Seconds() {
		//Should be updated regularly
		locker.Lock()
		Update()
		locker.Unlock()
		log.Printf("[recover][hasNodeid] activeNodeList update time %v\n", updateTime)
	}

	log.Printf("[recover][hasNodeid] online nodes %d\n", len(nodeList))

	temNodeList := nodeList

	for _, v := range temNodeList {
		//test
		//log.Println("[recover][hasNodeid] online_dnid=", v.NodeID, "request_dnid=", id)
		if v.NodeID == id {
			log.Println("[recover][hasNodeid] found: online dnid=",v.NodeID,"request_dnid=",id)
			return true
		}
	}
	return false
}
