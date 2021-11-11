package diskHash_test

import (
	"fmt"
	"testing"

	"github.com/yottachain/YTDataNode/diskHash"
	"github.com/yottachain/YTDataNode/instance"
)

var sn = instance.GetStorageNode()

func TestGetHash(t *testing.T) {
	fmt.Println(diskHash.GetHash(sn.YTFS()))
}
