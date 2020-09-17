package TaskPool

type TokenQueue struct {
	Num int32
	tc  chan *Token
}

type getTokenRequest struct {
	Level int32
	res   chan *Token
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.tc = make(chan *Token, Num)
	tq.Num = Num
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	return tq.tc
}

func (tq *TokenQueue) Add() {
	tk := NewToken()
	tk.Reset()
	select {
	case tq.tc <- tk:
	default:
	}
}
