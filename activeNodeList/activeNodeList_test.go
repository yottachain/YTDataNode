package activeNodeList

import (
	"crypto/rand"
	"fmt"
	"math/big"
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

type Cmap map[string]uint64

func TestRand(t *testing.T) {
	cmap := make(Cmap)
	wl := NewWeightNodeList(time.Second*3, time.Minute*5, 58, "")
	nodeList := wl.Get()

	for _, v := range nodeList {
		cmap[v.ID] = 0
	}
	randGetNode(cmap, nodeList)
	randGetNode(cmap, nodeList)
	randGetNode(cmap, nodeList)
	randGetNode(cmap, nodeList)
	fmt.Println()
	fmt.Println("-------------------------------------------------")
	countZero(cmap)
}

func randGetNode(cmap Cmap, nodeList []*Data) {
	var max = len(nodeList)
	for i := 0; i < 50000; i++ {
		index, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
		node := nodeList[index.Int64()]
		cmap[node.ID]++
	}
}

func countZero(cmap Cmap) {
	var c uint64
	for _, v := range cmap {
		if v == 0 {
			c++
		}
	}
	fmt.Println(c, len(cmap))
}
