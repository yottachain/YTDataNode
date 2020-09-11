package uploadTaskPool

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTDataNode/util"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

type delayStat struct {
	D int64
	C int64
	sync.RWMutex
}

func (s *delayStat) Avg() int64 {
	s.Lock()
	defer s.Unlock()

	if s.C == 0 {
		return 0
	}
	s.D = s.D / s.C
	s.C = 1
	return s.D
}

func (s *delayStat) Add(duration time.Duration) {
	s.Lock()
	defer s.Unlock()

	s.D += duration.Milliseconds()
	s.C++

	if s.C > 100 {
		s.D = s.D / s.C
		s.C = 1
	}
}

func NewStat() *delayStat {
	return &delayStat{}
}

type UploadTaskPool struct {
	tkc               chan *Token
	TTL               time.Duration `json:"ttl"`
	FillTokenInterval time.Duration `json:"fillTokenInterval"`
	sentToken         int64
	requestCount      int64
	NetLatency        *delayStat
	DiskLatency       *delayStat
	waitCount         int64
}

func New(size int, ttl time.Duration, fillInterval time.Duration) *UploadTaskPool {
	// 默认值
	if size == 0 {
		size = 500
	}

	upt := new(UploadTaskPool)

	//upt.tb = NewTokenBucket(size, ttl*time.Second)

	upt.FillTokenInterval = fillInterval
	upt.TTL = ttl
	upt.NetLatency = NewStat()
	upt.DiskLatency = NewStat()

	upt.Load()

	if upt.FillTokenInterval == 0 {
		upt.FillTokenInterval = 10
	}
	if upt.TTL == 0 {
		upt.TTL = 10
	}

	upt.TTL = time.Duration(config.Gconfig.TTL) * time.Second
	upt.MakeTokenQueue()

	return upt
}

func (upt *UploadTaskPool) Get(ctx context.Context, pid peer.ID, needStat bool) (*Token, error) {
	atomic.AddInt64(&upt.waitCount, 1)
	defer func() {
		atomic.AddInt64(&upt.waitCount, -1)
	}()

	// 如果队列长度大于等待token直接返回
	select {
	case tk := <-upt.tkc:
		tk.Reset()
		tk.PID = pid

		if needStat {
			atomic.AddInt64(&upt.sentToken, 1)
		} else {
			atomic.AddInt64(&statistics.DefaultStat.OtherTokenRequest, 1)
		}
		return tk, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("task busy")
	}
}

func (upt *UploadTaskPool) Check(tk *Token) bool {
	return time.Now().Sub(tk.Tm) < upt.TTL
}

func (upt *UploadTaskPool) Delete(tk *Token) bool {
	if atomic.LoadInt64(&upt.sentToken) < 1 {
		return false
	}
	atomic.AddInt64(&upt.requestCount, 1)
	return false
}

func (upt *UploadTaskPool) FillToken() {
	// 自动更改token速率
	upt.AutoChangeTokenInterval()

	for {
		<-time.After(upt.FillTokenInterval)

		tk := NewToken()
		tk.Reset()
		upt.tkc <- tk
	}
}

func (upt *UploadTaskPool) AutoChangeTokenInterval() {
	go func() {
		for {
			// 每10分钟衰减一次 token
			decreaseTd := time.Minute * time.Duration(config.Gconfig.Decrease)
			<-time.After(decreaseTd)
			sentTokenN := atomic.LoadInt64(&upt.sentToken)
			requestCountN := atomic.LoadInt64(&upt.requestCount)
			// 如果 发送的token 未消耗的 > 总量的 15% 减少token发放 百分之10
			if sentTokenN > 100 && ((sentTokenN - requestCountN) > sentTokenN*(100-config.Gconfig.DecreaseThreshold)/100) {
				log.Printf("[token] 触发token减少 [%d,%d] \n", sentTokenN, requestCountN)
				// 衰减量 是失败百分比
				decrement := upt.FillTokenInterval * time.Duration(sentTokenN-requestCountN) / time.Duration(sentTokenN)
				upt.ChangeTKFillInterval(upt.FillTokenInterval + decrement)
			}
		}
	}()
	go func() {
		for {
			increaseTd := time.Minute * time.Duration(config.Gconfig.Increase)
			// 小时增加一次token
			<-time.After(increaseTd)
			sentTokenN := atomic.LoadInt64(&upt.sentToken)
			requestCountN := atomic.LoadInt64(&upt.requestCount)
			// 如果 发送的token 未消耗的 < 总量的 5% 增加token发放 百分之20
			if (sentTokenN - requestCountN) < sentTokenN*config.Gconfig.IncreaseThreshold/100 {
				log.Printf("[token] 触发token增加 [%d,%d] \n", sentTokenN, requestCountN)
				upt.ChangeTKFillInterval(upt.FillTokenInterval - (upt.FillTokenInterval / 5))
			} else {
				log.Printf("[token] 未触发增加toekn %d,%d,%d\n", sentTokenN, requestCountN, config.Gconfig.IncreaseThreshold)
			}
		}
	}()
}

func (upt *UploadTaskPool) FreeTokenLen() int {
	return len(upt.tkc)
}

func (utp *UploadTaskPool) ChangeTKFillInterval(duration time.Duration) {
	makeZero := true
	if duration > (time.Second / time.Duration(config.Gconfig.MinToken)) {
		duration = time.Second / time.Duration(config.Gconfig.MinToken)
		makeZero = false
	}
	if duration < (time.Second / time.Duration(config.Gconfig.MaxToken)) {
		duration = time.Second / time.Duration(config.Gconfig.MaxToken)
		makeZero = false
	}
	utp.FillTokenInterval = duration
	if makeZero {
		atomic.StoreInt64(&utp.sentToken, 0)
		atomic.StoreInt64(&utp.requestCount, 0)
		atomic.StoreInt64(&statistics.DefaultStat.SaveRequestCount, 0)
		atomic.StoreInt64(&statistics.DefaultStat.RequestToken, 0)
		atomic.StoreInt64(&statistics.DefaultStat.OtherTokenRequest, 0)
	}

	utp.Save()
	os.Exit(0)
}

func (utp *UploadTaskPool) MakeTokenQueue() {
	size := time.Second / utp.FillTokenInterval * 3
	if size > 500 {
		size = 500
	}
	utp.tkc = make(chan *Token, size)
}

func (utp *UploadTaskPool) GetTFillTKSpeed() time.Duration {
	return time.Second / utp.FillTokenInterval
}

func (upt *UploadTaskPool) Save() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), ".utp_params.json"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	defer fl.Close()

	ec := json.NewEncoder(fl)
	err = ec.Encode(upt)
	if err != nil {
		log.Println(err)
		return
	}
}

func (upt *UploadTaskPool) Load() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), ".utp_params.json"), os.O_RDONLY, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	defer fl.Close()

	ec := json.NewDecoder(fl)
	if err := ec.Decode(upt); err != nil {
		log.Printf("[utp]读取历史记录失败 %v \n", err)
		return
	}
	log.Printf("[utp]读取历史记录成功 %v \n", upt)
}

func (upt *UploadTaskPool) GetParams() (int64, int64) {
	return atomic.LoadInt64(&upt.sentToken), atomic.LoadInt64(&upt.requestCount)
}

var utp *UploadTaskPool = New(500, time.Second*10, time.Millisecond*10)

func Utp() *UploadTaskPool {
	return utp
}
