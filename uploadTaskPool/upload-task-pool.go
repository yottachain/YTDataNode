package uploadTaskPool

import (
	"context"
	"fmt"
	log "github.com/yottachain/YTDataNode/logger"
	"sync/atomic"
	"time"
)

type UploadTaskPool struct {
	cap               int
	ttl               time.Duration
	fillTokenInterval time.Duration
	tokenBuket        *TokenBucket
	changeTime        atomic.Value
	runCount          int32
}

func New(size int, ttl time.Duration, fillTokenInterval time.Duration) *UploadTaskPool {
	utp := UploadTaskPool{
		size,
		ttl,
		fillTokenInterval,
		NewTokenBucket(size, ttl),
		atomic.Value{},
		0,
	}
	utp.changeTime.Store(time.Now())
	return &utp
}

func (utp *UploadTaskPool) FillQueue() {
	go func() {
		for {
			if atomic.LoadInt32(&utp.runCount) <= int32(utp.cap) {
				utp.tokenBuket.Put(NewToken())
			}
			//<-time.After(utp.fillTokenInterval)
		}
	}()
	go func() {
		for {
			log.Printf("[task pool]push token interval %f\n", utp.fillTokenInterval.Seconds())
			<-time.After(time.Minute)
		}
	}()
}

func (utp *UploadTaskPool) ChangePushTokenInterval(d time.Duration) {
	if d < 10*time.Millisecond {
		utp.fillTokenInterval = 10 * time.Millisecond
	} else if d > 1000*time.Millisecond {
		utp.fillTokenInterval = 1000 * time.Millisecond
	} else {
		utp.fillTokenInterval = d
	}
}

func (utp *UploadTaskPool) GetTokenFromWaitQueue(ctx context.Context) (*Token, error) {
	tk := utp.tokenBuket.Get(ctx)
	if tk != nil {
		return tk, nil
	} else {

		if time.Now().Sub(utp.changeTime.Load().(time.Time)) > time.Second*10 {
			utp.ChangePushTokenInterval(utp.fillTokenInterval - 1*time.Millisecond)
			utp.changeTime.Store(time.Now())
			log.Printf("[task pool]change fillTokenInterval %f\n", utp.fillTokenInterval.Seconds())
		}

		return nil, fmt.Errorf("upload queue bus")
	}
}

func (utp *UploadTaskPool) Check(tk *Token) bool {
	res := utp.tokenBuket.Check(tk)
	if res == false && time.Now().Sub(utp.changeTime.Load().(time.Time)) > time.Second*10 {
		utp.ChangePushTokenInterval(utp.fillTokenInterval + 10*time.Millisecond)
		log.Printf("[task pool]change fillTokenInterval %f\n", utp.fillTokenInterval.Seconds())
		utp.changeTime.Store(time.Now())
	}
	return res
}

func (utp *UploadTaskPool) Do() {
	log.Printf("[task pool][task start]task count %d\n", utp.runCount)
	atomic.AddInt32(&utp.runCount, 1)
}
func (utp *UploadTaskPool) Done() {
	log.Printf("[task pool][task end]task count %d\n", utp.runCount)
	atomic.AddInt32(&utp.runCount, -1)
}
