package statistics

import (
	"fmt"
	"testing"
	"time"
)

func TestNewWaitCount(t *testing.T) {
	wc := NewWaitCount(5)
	go func() {
		for {
			<-time.After(time.Second)
			wc.Remove()
		}
	}()
	for {
		wc.Add()
		fmt.Println(wc.Len())
	}
}
