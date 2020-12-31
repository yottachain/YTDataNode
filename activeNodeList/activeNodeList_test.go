package activeNodeList

import (
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	fmt.Println(GetNodeListByTimeAndGroupSize(time.Minute*10, 1))
}
