package uploadTaskPool

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"time"
)

type UploadTaskPool struct {
	tkc               chan *Token
	tb                *TokenBucket
	fillTokenInterval time.Duration
}

func New(size int, ttl time.Duration, fillInterval time.Duration) *UploadTaskPool {
	// 默认值
	if size == 0 {
		size = 500
	}
	if fillInterval == 0 {
		fillInterval = 10
	}
	if ttl == 0 {
		ttl = 10
	}

	upt := new(UploadTaskPool)

	upt.tb = NewTokenBucket(size, ttl*time.Second)
	upt.tkc = make(chan *Token, size)
	upt.fillTokenInterval = fillInterval

	return upt
}

func (upt *UploadTaskPool) Get(ctx context.Context, pid peer.ID) (*Token, error) {
	select {
	case tk := <-upt.tkc:
		tk.Reset()
		tk.PID = pid
		return tk, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("task busy")
	}
}

func (upt *UploadTaskPool) Check(tk *Token) bool {
	return upt.tb.Check(tk)
}

func (upt *UploadTaskPool) Delete(tk *Token) bool {
	return upt.tb.Delete(tk)
}

func (upt *UploadTaskPool) FillToken(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(upt.fillTokenInterval):
			if tk := upt.tb.Get(); tk != nil {
				upt.tkc <- tk
				//fmt.Println("fill token", upt.tb.Len())
			}
		}
	}
}

func (upt *UploadTaskPool) FreeTokenLen() int {
	return len(upt.tkc)
}
