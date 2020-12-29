package diskHash

import (
	"fmt"
	"github.com/yottachain/YTDataNode/instance"
	"testing"
)

var sn = instance.GetStorageNode()

func TestGetHash(t *testing.T) {
	fmt.Println(GetHash(sn.YTFS()))
}
