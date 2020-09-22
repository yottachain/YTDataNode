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
	if level == 1 {
		tc := make(chan *Token, 1)
		select {
		case tc <- <-tq.tc2:
		case tc <- <-tq.tc:

		default:

		}
		return tc
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
