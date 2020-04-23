package uploadTaskPool

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/yottachain/YTDataNode/logger"
	"time"
)

type UploadTaskPool struct {
	tkc               chan *Token
	tb                *TokenBucket
	fillTokenInterval time.Duration
}

func New(size int, ttl time.Duration, fillInterval time.Duration) *UploadTaskPool {
	upt := new(UploadTaskPool)

	upt.tb = NewTokenBucket(size, ttl)
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
		case <-time.After(upt.fillTokenInterval):
			if tk := upt.tb.Get(); tk != nil {
				upt.tkc <- tk
				log.Printf("fill token tokenbucket len(%d)", upt.tb.Len())
			}
		}
	}
}
