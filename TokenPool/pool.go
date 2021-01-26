package TokenPool

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	"os"
	"path"
	"sync/atomic"
	"time"
)

type TokenPool struct {
	name              string
	tkc               *TokenQueue
	TTL               time.Duration `json:"ttl"`
	FillTokenInterval time.Duration `json:"fillTokenInterval"`
	NetLatency        *delayStat
	DiskLatency       *delayStat
	waitCount         int64
	GetRate           func() int64 `json:"-"`
	changeHandler     func(pt *TokenPool)
}

func New(name string, size int, ttl time.Duration, fillInterval time.Duration) *TokenPool {
	// 默认值
	if size == 0 {
		size = 500
	}

	tp := new(TokenPool)
	tp.name = name
	tp.GetRate = func() int64 {
		return 100
	}

	tp.FillTokenInterval = fillInterval
	tp.TTL = ttl

	tp.Load()
	tp.NetLatency = NewStat()
	tp.DiskLatency = NewStat()

	if tp.FillTokenInterval == 0 {
		tp.FillTokenInterval = 10
	}
	if tp.TTL == 0 {
		tp.TTL = 10
	}

	tp.TTL = time.Duration(config.Gconfig.TTL) * time.Second
	tp.MakeTokenQueue()

	return tp
}

func (pt *TokenPool) Get(ctx context.Context, pid peer.ID, level int32) (*Token, error) {
	atomic.AddInt64(&pt.waitCount, 1)
	defer func() {
		atomic.AddInt64(&pt.waitCount, -1)
	}()

	// 等待队列长度
	ql := int64(time.Duration(config.Gconfig.TokenWait)*time.Millisecond/pt.FillTokenInterval) / 2
	if ql < 1 {
		ql = 1
	}

	// 如果队列长度大于等待token直接返回
	if atomic.LoadInt64(&pt.waitCount) > ql {
		return nil, fmt.Errorf("token buys queue len (%d/%d)", atomic.LoadInt64(&pt.waitCount), ql)
	}
	select {
	case tk := <-pt.tkc.Get(level):
		if tk == nil {
			return nil, fmt.Errorf("token busy")
		}
		tk.PID = pid
		tk.Reset()
		return tk, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("ctx time out")
		//default:
		//	return nil, fmt.Errorf("token busy3")
	}
}

func (pt *TokenPool) Check(tk *Token) bool {
	return time.Now().Sub(tk.Tm) < pt.TTL
}

func (pt *TokenPool) Delete(tk *Token) bool {
	return false
}

func (pt *TokenPool) FillToken() {
	//自动更改token速率
	//pt.AutoChangeTokenInterval()

	for {
		startTime := time.Now()
		<-time.After(pt.FillTokenInterval)
		//time2 := time.Now()
		d := time.Now().Sub(startTime)
		c := 0
		for {
			pt.tkc.Add()
			c += 1
			d = d - pt.FillTokenInterval
			if pt.FillTokenInterval/2 <= 0 || d < pt.FillTokenInterval/2 {
				break
			}
		}
	}
}

func (pt *TokenPool) AutoChangeTokenInterval(IncreaseThreshold, Increase, DecreaseThreshold, Decrease int64) {
	go func() {
		for {
			// 每10分钟衰减一次 token
			decreaseTd := time.Minute * time.Duration(Decrease)
			<-time.After(decreaseTd)
			// 如果 发送的token 未消耗的 > 总量的 15% 减少token发放 百分之10
			rate := pt.GetRate()
			if rate == -1 {
				continue
			}
			if rate < DecreaseThreshold {
				log.Printf("[token] 触发token减少 [%d][%d,%d] \n", rate, DecreaseThreshold, IncreaseThreshold)
				// 衰减量 是失败百分比
				decrement := pt.FillTokenInterval * (time.Duration(DecreaseThreshold) - time.Duration(rate)) / 100
				pt.ChangeTKFillInterval(pt.FillTokenInterval + decrement)
			}
		}
	}()
	go func() {
		for {
			increaseTd := time.Minute * time.Duration(Increase)
			// 小时增加一次token
			<-time.After(increaseTd)
			rate := pt.GetRate()
			if rate == -1 {
				continue
			}
			// 如果 发送的token 未消耗的 < 总量的 5% 增加token发放 百分之20
			if rate > IncreaseThreshold {
				log.Printf("[token] 触发token增加 [%d][%d,%d] \n", rate, DecreaseThreshold, IncreaseThreshold)
				decrement := pt.FillTokenInterval * (time.Duration(rate) - time.Duration(IncreaseThreshold)) / 100
				pt.ChangeTKFillInterval(pt.FillTokenInterval - decrement)
			}
		}
	}()
}

func (pt *TokenPool) FreeTokenLen() int {
	return pt.tkc.Len()
}

func (pt *TokenPool) ChangeTKFillInterval(duration time.Duration) {
	makeZero := true
	if duration > (time.Second / time.Duration(config.Gconfig.MinToken)) {
		duration = time.Second / time.Duration(config.Gconfig.MinToken)
		makeZero = false
	}
	if duration < (time.Second / time.Duration(config.Gconfig.MaxToken)) {
		duration = time.Second / time.Duration(config.Gconfig.MaxToken)
		makeZero = false
	}
	pt.FillTokenInterval = duration
	if makeZero {
		if pt.changeHandler != nil {
			pt.changeHandler(pt)
		}
	}

	pt.Save()
	//pt.MakeTokenQueue()
}

func (pt *TokenPool) OnChange(handler func(pt *TokenPool)) {
	pt.changeHandler = handler
}

func (pt *TokenPool) MakeTokenQueue() {
	size := time.Second / pt.FillTokenInterval * 3
	if size > 500 {
		size = 500
	}
	pt.tkc = NewTokenQueue(int32(config.Gconfig.MaxToken))
}

func (pt *TokenPool) GetTFillTKSpeed() time.Duration {
	return time.Second / pt.FillTokenInterval
}

func (pt *TokenPool) Save() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), pt.name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("[task pool]", err)
		return
	}
	defer fl.Close()

	ec := json.NewEncoder(fl)
	err = ec.Encode(pt)
	if err != nil {
		log.Println(err)
		return
	}
}

func (pt *TokenPool) Load() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), pt.name), os.O_RDONLY, 0644)
	if err != nil {
		log.Println("[task pool]", err)
		return
	}
	defer fl.Close()

	ec := json.NewDecoder(fl)
	if err := ec.Decode(pt); err != nil {
		log.Printf("[utp]读取历史记录失败 %v \n", err)
		return
	}
	if pt.FillTokenInterval < time.Millisecond {
		pt.FillTokenInterval = 10 * time.Millisecond
	}
	log.Printf("[utp]读取历史记录成功 %v \n", pt)
}

func (pt *TokenPool) GetParams() (int64, int64) {
	return 0, 0
}

var UploadTP *TokenPool = New(".utp_params.json", 500, time.Second*10, time.Millisecond*10)
var DownloadTP *TokenPool = New(".dtp_params.json", 500, time.Second*10, time.Millisecond*10)

// 上行token任务池
func Utp() *TokenPool {
	return UploadTP
}

// 下行token任务池
func Dtp() *TokenPool {
	return DownloadTP
}
