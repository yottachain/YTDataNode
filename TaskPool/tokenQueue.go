package TaskPool

type TokenQueue struct {
	tc  chan *Token
	tc2 chan *Token
}

func (tq *TokenQueue) Len() int {
	return len(tq.tc) + len(tq.tc2)
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.tc = make(chan *Token, Num)
	tq.tc2 = make(chan *Token)
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	select {
	case tq.tc2 <- <-tq.tc:
	default:
	}
	if level == 1 {
		return tq.tc2
	}
	return tq.tc
}

func (tq *TokenQueue) Add() {
	tk := NewToken()
	tk.Reset()
	select {
	case tq.tc2 <- tk:
	case tq.tc <- tk:
	default:
	}
}
