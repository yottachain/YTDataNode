package TaskPool

type TokenQueue struct {
	Num          int32
	LevelRequest map[int32]chan *Token
}

func NewTokenQueue(Num int32) *TokenQueue {
	tq := new(TokenQueue)
	tq.LevelRequest = make(map[int32]chan *Token)
	tq.Num = Num
	return tq
}

func (tq *TokenQueue) Get(level int32) chan *Token {
	lm, ok := tq.LevelRequest[level]
	if !ok {
		lm = make(chan *Token, tq.Num)
		tq.LevelRequest[level] = lm
	}
	return lm
}

func (tq *TokenQueue) Add() chan struct{} {
	var res chan struct{}
	var maxLevel int32
	for i, lq := range tq.LevelRequest {
		if i >= maxLevel && len(lq) > 0 {
			func(maxLevel *int32, i int32) {
				*maxLevel = i
			}(&maxLevel, i)
		}
	}
	tq.LevelRequest[maxLevel] <- NewToken()
	res <- struct{}{}
	return res
}
