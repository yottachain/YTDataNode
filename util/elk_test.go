package util

import (
	"testing"
)

func TestNewElkClient(t *testing.T) {
	clt := NewElkClient("test")
	clt.AddLogAsync(struct {
		Test string
	}{"1111"})
}
