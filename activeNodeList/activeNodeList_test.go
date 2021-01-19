package activeNodeList

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	nodeList := GetWeightNodeList(GetNodeListByTimeAndGroupSize(time.Minute*10, 58))
	for _, v := range nodeList {
		fmt.Println(v.ID, v.IP, v.Weight, v.WInt)
	}
	fmt.Println(len(nodeList))
}

func TestBuffer(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 2))
	for {
		n, err := fmt.Fprintln(buf, "11")
		fmt.Println(n, err)
	}
}
