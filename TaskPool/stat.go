package TaskPool

import (
	"sync"
	"time"
)

type delayStat struct {
	D int64
	C int64
	sync.RWMutex
}

func (s *delayStat) Avg() int64 {
	s.Lock()
	defer s.Unlock()

	if s.C == 0 {
		return 0
	}
	s.D = s.D / s.C
	s.C = 1
	return s.D
}

func (s *delayStat) Add(duration time.Duration) {
	s.Lock()
	defer s.Unlock()

	s.D += duration.Milliseconds()
	s.C++

	if s.C > 100 {
		s.D = s.D / s.C
		s.C = 1
	}
}

func NewStat() *delayStat {
	return &delayStat{}
}
