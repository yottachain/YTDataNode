package activeNodeList

import (
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	fmt.Println(len(GetNodeListByTimeAndGroupSize(time.Minute*10, 58)), len(GetNodeList()), len(GetGroupList()))
}
