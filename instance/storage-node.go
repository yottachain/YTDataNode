package instance

import (
	node "github.com/yottachain/YTDataNode"
	"github.com/yottachain/YTDataNode/config"
	. "github.com/yottachain/YTDataNode/storageNodeInterface"
)

var sn StorageNode

// GetStorageNode 获取StorageNode
func GetStorageNode() StorageNode {
	if sn == nil {
		cfg, err := config.ReadConfig()
		if err != nil {
			panic(err)
		}
		sn, err = node.NewStorageNode(cfg)
		if err != nil {
			panic(err)
		}
	}
	return sn
}
