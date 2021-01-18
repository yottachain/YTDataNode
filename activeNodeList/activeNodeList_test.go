package activeNodeList

import (
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	nodeList := GetWeightNodeList(GetNodeListByTimeAndGroupSize(time.Minute*10, 58))
	for _, v := range nodeList {
		fmt.Println(v.ID, v.Weight, v.WInt)
	}
	fmt.Println(len(nodeList))
}
