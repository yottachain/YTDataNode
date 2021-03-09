package activeNodeList

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	wl := NewWeightNodeList(time.Second*3, time.Minute*5, 58, "")
	nodeList := wl.Get()
	//for _, v := range nodeList {
	//	fmt.Println(v.ID, v.IP, v.Weight, v.WInt)
	//}
	fmt.Println(len(nodeList))
	<-time.After(time.Second * 3)
	nodeList = wl.Get()
	for k, v := range nodeList {
		if v.ID == "114" {
			println(k, v.ID, v.Weight, v.WInt, v.Group)
		}
	}

}
func TestRand(t *testing.T) {
	var max = 131585
	i := 0
	for ; i < 131585; i++ {
		if rand.Intn(max) == 71392 {
			println(i)
			return
		}
	}
}
