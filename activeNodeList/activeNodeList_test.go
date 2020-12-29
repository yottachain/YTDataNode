package activeNodeList

import (
	"fmt"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	fmt.Println(GetNodeListByTime(time.Minute * 10))
}
