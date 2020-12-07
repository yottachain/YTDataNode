package statistics

import (
	"encoding/json"
	"github.com/libp2p/go-libp2p-core/peer"
	recover2 "github.com/yottachain/YTDataNode/recover"
	"sync"
	"time"
)

type Stat struct {
	SaveRequestCount       int64         `json:"SaveRequestCount"` // 上传请求数量
	SaveSuccessCount       int64         `json:"SaveSuccessCount"` // 保存成功数量
	YTFSErrorCount         uint64        `json:"ytfs_error_count"`
	TokenQueueLen          int           `json:"TokenQueueLen"`
	AvailableTokenNumber   int           `json:"AvailableTokenNumber""`
	SentToken              int64         `json:"SentToken"` // 发送token数量
	UseKvDb                bool          `json:"UseKvDb"`
	TokenFillSpeed         time.Duration `json:"TokenFillSpeed"`
	UpTime                 int64         `json:"UpTime"`
	Connection             int           `json:"Connection"`
	AverageToken           int64         `json:"AverageToken"`
	SentTokenNum           int64
	ReportTime             time.Time
	RequestToken           int64
	RequestDownloadToken   int64
	NetLatency             int64 // 上传网路延迟
	DiskLatency            int64 // 上传硬盘延迟
	GconfigMd5             string
	RebuildShardStat       *recover2.RecoverStat
	DownloadTokenFillSpeed time.Duration
	SentDownloadToken      int64 // 下载发送token数量
	DownloadSuccessCount   int64 // 下载成功数量
	SentDownloadTokenNum   int64
	AverageDownloadToken   int64
	DownloadNetLatency     int64 // 下载网络延迟
	DownloadDiskLatency    int64
	Ban                    bool
	DownloadData404        int64
	sync.RWMutex
}

func (s *Stat) JsonEncode() []byte {
	var res []byte

	s.RLock()
	defer s.RUnlock()
	so := *s

	buf, err := json.Marshal(so)
	if err == nil {
		res = buf
	}

	return res
}

func (s *Stat) String() string {

	var res = ""

	buf := s.JsonEncode()
	if buf != nil {
		res = string(buf)
	}

	return res
}
func (s *Stat) Mean() {
	s.Lock()
	defer s.Unlock()

	td := int64(time.Now().Sub(s.ReportTime).Seconds())
	if td <= 0 {
		return
	}

	s.AverageToken = s.SentTokenNum / td
	s.AverageDownloadToken = s.SentDownloadTokenNum / td

	s.SentTokenNum = 0
	s.SentDownloadTokenNum = 0
	s.ReportTime = time.Now()
}

var DefaultStat Stat
var ConnectCountMap = make(map[peer.ID]int)
var ConnectMapMux = &sync.Mutex{}

func AddCounnectCount(id peer.ID) {
	ConnectMapMux.Lock()
	defer ConnectMapMux.Unlock()

	if _, ok := ConnectCountMap[id]; ok {
		ConnectCountMap[id]++
	} else {
		ConnectCountMap[id] = 0
	}
}

func SubCounnectCount(id peer.ID) {
	ConnectMapMux.Lock()
	defer ConnectMapMux.Unlock()

	if _, ok := ConnectCountMap[id]; ok {
		ConnectCountMap[id]--
		if ConnectCountMap[id] <= 0 {
			delete(ConnectCountMap, id)
		}
	}
}

func GetConnectionNumber() int {
	ConnectMapMux.Lock()
	defer ConnectMapMux.Unlock()

	return len(ConnectCountMap)
}

func InitDefaultStat() {
	DefaultStat.UpTime = time.Now().Unix()
	DefaultStat.ReportTime = time.Now()

	//go func() {
	//	fl, err := os.OpenFile(".stat", os.O_CREATE|os.O_RDONLY, 0644)
	//	if err != nil {
	//		log.Println("[stat]", err.Error())
	//		return
	//	}
	//
	//	buf, err := ioutil.ReadAll(fl)
	//	if err != nil {
	//		log.Println("[stat]", err.Error())
	//		return
	//	}
	//	fl.Close()
	//
	//	if len(buf) > 0 {
	//		var ns Stat
	//		if err := json.Unmarshal(buf, &ns); err != nil {
	//			log.Println("[stat]", err.Error())
	//			return
	//		}
	//		DefaultStat = ns
	//	}
	//}()
	//
	//go func() {
	//	for {
	//		<-time.After(time.Second)
	//		fl2, err := os.OpenFile(".stat", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	//		buf := DefaultStat.JsonEncode()
	//		if err != nil || buf == nil {
	//			log.Println("[stat] write", err.Error())
	//		} else {
	//			fl2.Write(buf)
	//		}
	//
	//		fl2.Close()
	//	}
	//}()
}
