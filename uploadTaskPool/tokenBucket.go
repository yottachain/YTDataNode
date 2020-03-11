package uploadTaskPool

import (
	"context"
	"time"
)

type TokenBucket struct {
	tkchan chan *Token
	cap    int
	ttl    time.Duration
}

func NewTokenBucket(size int, ttl time.Duration) *TokenBucket {
	var tb = TokenBucket{
		make(chan *Token, size),
		size,
		ttl,
	}
	return &tb
}

func (tb *TokenBucket) Get(ctx context.Context) *Token {
	var tk *Token
	select {
	case tk = <-tb.tkchan:
		tk.Tm = time.Now()
	//case <-ctx.Done():
	//	tk = nil
	default:
		tk = nil
	}
	return tk
}

func (tb *TokenBucket) Put(tk *Token) bool {
	if len(tb.tkchan) < tb.cap {
		tk.Reset()
		tb.tkchan <- tk
		return true
	} else {
		return false
	}
}

func (tb *TokenBucket) Check(tk *Token) bool {
	return !tk.IsOuttime(tb.ttl)
}
