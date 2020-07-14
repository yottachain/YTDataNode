package uploadTaskPool

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
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
	D time.Duration
	C int64
	sync.RWMutex
}

func (s *delayStat) Avg() time.Duration {
	s.Lock()
	defer s.Unlock()

	if s.C == 0 {
		return 0
	}
	defer func() {
		s.C = 1
	}()
	return s.D / time.Duration(s.C)
}

func (s *delayStat) Add(duration time.Duration) {
	s.Lock()
	defer s.Unlock()

	s.D += duration
	s.C++
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
	Delay             *delayStat
}

func New(size int, ttl time.Duration, fillInterval time.Duration) *UploadTaskPool {
	// 默认值
	if size == 0 {
		size = 500
	}

	upt := new(UploadTaskPool)

	//upt.tb = NewTokenBucket(size, ttl*time.Second)
	upt.tkc = make(chan *Token, time.Second/fillInterval)
	upt.FillTokenInterval = fillInterval
	upt.TTL = ttl
	upt.Delay = NewStat()

	upt.Load()

	if upt.FillTokenInterval == 0 {
		upt.FillTokenInterval = 10
	}
	if upt.TTL == 0 {
		upt.TTL = 10
	}

	return upt
}

func (upt *UploadTaskPool) Get(ctx context.Context, pid peer.ID) (*Token, error) {
	select {
	case tk := <-upt.tkc:
		tk.Reset()
		tk.PID = pid

		atomic.AddInt64(&upt.sentToken, 1)
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
	upt.Delay.Add(time.Now().Sub(tk.Tm))
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
			decreaseTd := time.Minute * 5
			<-time.After(decreaseTd)
			sentTokenN := atomic.LoadInt64(&upt.sentToken)
			requestCountN := atomic.LoadInt64(&upt.requestCount)
			// 如果 发送的token 未消耗的 > 总量的 15% 减少token发放 百分之10
			if (sentTokenN - requestCountN) > sentTokenN*3/20 {
				log.Printf("[token] 触发token减少 [%d,%d] \n", sentTokenN, requestCountN)
				upt.ChangeTKFillInterval(upt.FillTokenInterval + (upt.FillTokenInterval / 10))
			}
		}
	}()
	go func() {
		for {
			increaseTd := time.Minute * 30
			// 小时增加一次token
			<-time.After(increaseTd)
			sentTokenN := atomic.LoadInt64(&upt.sentToken)
			requestCountN := atomic.LoadInt64(&upt.requestCount)
			// 如果 发送的token 未消耗的 < 总量的 5% 增加token发放 百分之20
			if (sentTokenN - requestCountN) < sentTokenN*1/20 {
				log.Printf("[token] 触发token增加 [%d,%d] \n", sentTokenN, requestCountN)
				upt.ChangeTKFillInterval(upt.FillTokenInterval - (upt.FillTokenInterval / 5))
			}
		}
	}()
}

func (upt *UploadTaskPool) FreeTokenLen() int {
	return len(upt.tkc)
}

func (utp *UploadTaskPool) ChangeTKFillInterval(duration time.Duration) {
	if duration > (time.Millisecond * 50) {
		duration = time.Millisecond * 50
	}
	if duration < (time.Millisecond * 2) {
		duration = time.Millisecond * 2
	}
	utp.FillTokenInterval = duration
	atomic.StoreInt64(&utp.sentToken, 0)
	atomic.StoreInt64(&utp.requestCount, 0)
	atomic.StoreInt64(&statistics.DefaultStat.SaveRequestCount, 0)
	atomic.StoreInt64(&statistics.DefaultStat.RequestToken, 0)
	size := time.Second / duration * 10
	if size > 500 {
		size = 500
	}
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
