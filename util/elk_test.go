package util

import (
	"testing"
	"time"
)

func TestNewElkClient(t *testing.T) {
	a := true
	clt := NewElkClient("test", &a)
	clt.AddLogAsync(struct {
		Test string
	}{"1111"})
	<-time.After(time.Second * 10)
}
