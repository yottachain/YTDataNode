package uploadTaskPool

import (
	log "github.com/yottachain/YTDataNode/logger"
	"sync"
	"time"
)

type TokenBucket struct {
	tks []*Token
	cap int
	ttl time.Duration
	sync.Mutex
}

func NewTokenBucket(size int, ttl time.Duration) *TokenBucket {
	var tb = TokenBucket{
		make([]*Token, size),
		size,
		ttl,
		sync.Mutex{},
	}
	return &tb
}

func (tb *TokenBucket) Get() *Token {
	tb.Lock()
	defer tb.Unlock()

	for k, v := range tb.tks {
		if v == nil || v.IsOuttime(tb.ttl) {
			tb.tks[k] = NewToken()
			tb.tks[k].Reset()
			return tb.tks[k]
		}
	}
	return nil
}

func (tb *TokenBucket) Check(tk *Token) bool {
	tb.Lock()
	defer tb.Unlock()

	for k, v := range tb.tks {
		if v != nil && tk.String() == v.String() {
			log.Println("[uploadTaskPool] check token success", tk.String(), v.String())
			tb.tks[k] = nil
			if tk.IsOuttime(tb.ttl) {
				return false
			}
			return true
		}
	}
	return false
}

func (tb *TokenBucket) Len() int {
	var l int
	for _, v := range tb.tks {
		if v != nil {
			l = l + 1
		}
	}
	return l
}
