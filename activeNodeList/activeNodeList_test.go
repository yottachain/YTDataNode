package activeNodeList

import (
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	wl := NewWeightNodeList(time.Second*3, time.Minute*5, 58, nil)
	nodeList := wl.Get()
	//for _, v := range nodeList {
	//	fmt.Println(v.ID, v.IP, v.Weight, v.WInt)
	//}
	fmt.Println(len(nodeList))
	<-time.After(time.Second * 3)
	nodeList = wl.Get()
	fmt.Println(len(nodeList))

}
