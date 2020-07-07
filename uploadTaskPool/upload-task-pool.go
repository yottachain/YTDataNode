package uploadTaskPool

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/yottachain/YTDataNode/logger"
	"sync/atomic"
	"time"
)

type UploadTaskPool struct {
	tkc               chan *Token
	ttl               time.Duration
	fillTokenInterval time.Duration
	sentToken         int64
}

func New(size int, ttl time.Duration, fillInterval time.Duration) *UploadTaskPool {
	// 默认值
	if size == 0 {
		size = 200
	}
	if fillInterval == 0 {
		fillInterval = 10
	}
	if ttl == 0 {
		ttl = 10
	}

	upt := new(UploadTaskPool)

	//upt.tb = NewTokenBucket(size, ttl*time.Second)
	upt.tkc = make(chan *Token, size)
	upt.fillTokenInterval = fillInterval
	upt.ttl = ttl

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
	return time.Now().Sub(tk.Tm) < upt.ttl
}

func (upt *UploadTaskPool) Delete(tk *Token) bool {
	atomic.AddInt64(&upt.sentToken, -1)
	return false
}

func (upt *UploadTaskPool) FillToken() {
	// 自动更改token速率
	upt.AutoChangeTokenInterval()

	for {
		<-time.After(upt.fillTokenInterval)

		tk := NewToken()
		tk.Reset()
		upt.tkc <- tk
	}
}

func (upt *UploadTaskPool) AutoChangeTokenInterval() {
	go func() {
		for {
			// 每10分钟衰减一次 token
			decreaseTd := time.Minute * 10
			<-time.After(decreaseTd)
			// 如果 发送的token 未消耗的 > 总量的 15% 减少token发放 百分之10
			if atomic.LoadInt64(&upt.sentToken) > int64(decreaseTd/upt.fillTokenInterval*3/20) {
				log.Printf("[token] 触发token减少 未消耗token %d\n", atomic.LoadInt64(&upt.sentToken))
				upt.ChangeTKFillInterval(upt.fillTokenInterval + (upt.fillTokenInterval / 10))
			}
		}
	}()
	go func() {
		for {
			increaseTd := time.Hour
			// 小时增加一次token
			<-time.After(increaseTd)
			// 如果 发送的token 未消耗的 < 总量的 5% 增加token发放 百分之20
			if atomic.LoadInt64(&upt.sentToken) < int64(increaseTd/upt.fillTokenInterval*19/20) {
				log.Printf("[token] 触发token增加 未消耗token %d\n", atomic.LoadInt64(&upt.sentToken))
				upt.ChangeTKFillInterval(upt.fillTokenInterval - (upt.fillTokenInterval / 5))
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
	if duration < (time.Millisecond * 5) {
		duration = time.Millisecond * 5
	}
	utp.fillTokenInterval = duration
	atomic.StoreInt64(&utp.sentToken, 0)
}
