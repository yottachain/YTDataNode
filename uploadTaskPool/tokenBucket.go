package uploadTaskPool

import (
	"bytes"
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
		// pid != "" 代表token被发放到具体客户端
		if v == nil || (v.PID != "" && v.IsOuttime(tb.ttl)) {
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

	for _, v := range tb.tks {
		if v != nil && bytes.Equal(tk.UUID.Bytes(), v.UUID.Bytes()) {
			if tk.IsOuttime(tb.ttl) {
				return false
			}
			return true
		}
	}
	return false
}

func (tb *TokenBucket) findToken(tk *Token) int {
	for k, v := range tb.tks {
		if v != nil && bytes.Equal(tk.UUID.Bytes(), v.UUID.Bytes()) {
			if tk.IsOuttime(tb.ttl) {
				return -1
			}
			return k
		}
	}
	return -1
}

func (tb *TokenBucket) Delete(tk *Token) bool {
	tb.Lock()
	defer tb.Unlock()

	for k, v := range tb.tks {
		if v != nil && bytes.Equal(tk.UUID.Bytes(), v.UUID.Bytes()) {
			tb.tks[k] = nil
			if tk.IsOuttime(tb.ttl) {
				return false
			}
			return true
		}
	}
	return false
}

func (tb *TokenBucket) FreeTokenLen() int {
	tb.Lock()
	defer tb.Unlock()

	var l int
	for _, v := range tb.tks {
		if v == nil || v.PID == "" || (v.PID != "" && v.IsOuttime(tb.ttl)) {
			l++
		}
	}
	return l
}
