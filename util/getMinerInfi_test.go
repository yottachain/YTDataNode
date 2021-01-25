package util

import (
	"fmt"
	"testing"
)

func TestGetMinerInfo(t *testing.T) {
	mi := &MinerInfo{ID: 8334}
	fmt.Println(mi.IsNoSpace(10), mi)
}
