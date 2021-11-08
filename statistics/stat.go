package statistics

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/yottachain/YTDataNode/logger"
	ytfsOpts "github.com/yottachain/YTFS/opt"
)

type Stat struct {
	RXRequest            int64         `json:"RXRequest"` // 上传请求数量
	RXSuccess            int64         `json:"RXSuccess"` // 保存成功数量，改为上传仅rpc接口成功数量统计
	YTFSErrorCount       uint64        `json:"ytfs_error_count"`
	TokenQueueLen        int           `json:"TokenQueueLen"`
	AvailableTokenNumber int           `json:"AvailableTokenNumber"`
	RXToken              int64         `json:"RXToken"` // 发送token数量，改为仅RPC调用成功发送token数量
	UseKvDb              bool          `json:"UseKvDb"`
	RXTokenFillRate      time.Duration `json:"RXTokenFillRate"`
	UpTime               int64         `json:"UpTime"`
	Connection           uint64        `json:"Connection"`
	RXAverageToken       int64         `json:"RXAverageToken"`
	SentTokenNum         int64         `json:"-"`
	ReportTime           time.Time     `json:"-"`
	ReportTimeUnix       int64
	RXRequestToken       int64
	TXRequestToken       int64
	RXNetLatency         int64 // 上传网路延迟
	RXDiskLatency        int64 // 上传硬盘延迟
	GconfigMd5           string
	RebuildShardStat     *RecoverStat
	TXTokenFillRate      time.Duration
	TXToken              int64 // 下载发送token数量，改为仅RPC接口
	TXSuccess            int64 // 下载成功数量，改为仅RPC接口
	SentDownloadTokenNum int64 `json:"-"`
	TXAverageToken       int64
	TXNetLatency         int64 // 下载网络延迟
	TXDiskLatency        int64
	RXTest               *RateCounter
	TXTest               *RateCounter
	RXTestConnectRate    RateCounter
	TXTestConnectRate    RateCounter
	//RandDownloadCount      int64 // 仅矿机间下载计数
	//RandDownloadSuccess    int64 // 仅矿机间下载成功计数
	Ban             bool
	DownloadData404 int64
	MediumError     int64
	NoSpaceError    int64
	RangeFullError  int64
	IndexDBOpt      *ytfsOpts.Options
	sync.RWMutex
}

func (s *Stat) JsonEncode() []byte {
	var res []byte

	s.RLock()
	defer s.RUnlock()
	so := *s

	buf, err := json.Marshal(so)
	if err == nil {
		log.Println("json err:", err)
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

	s.RXAverageToken = s.SentTokenNum / td
	s.TXAverageToken = s.SentDownloadTokenNum / td
	s.reset()
}

func (s *Stat) reset() {
	s.SentTokenNum = 0
	s.SentDownloadTokenNum = 0

	s.ReportTime = time.Now()
	s.ReportTimeUnix = time.Now().Unix()
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
	DefaultStat.RXTest = new(RateCounter)
	DefaultStat.TXTest = new(RateCounter)
}
