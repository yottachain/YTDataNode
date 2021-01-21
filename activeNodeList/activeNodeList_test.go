package activeNodeList

import (
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	wl := NewWeightNodeList(func() []*Data {
		return GetNodeListByTimeAndGroupSize(time.Minute*10, 58)
	})
	nodeList := wl.Get()
	for _, v := range nodeList {
		fmt.Println(v.ID, v.IP, v.Weight, v.WInt)
	}
	fmt.Println(len(nodeList))
}
