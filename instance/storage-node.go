package instance

import (
	node "github.com/yottachain/YTDataNode"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	. "github.com/yottachain/YTDataNode/storageNodeInterface"
)

var sn StorageNode

// GetStorageNode 获取StorageNode
func GetStorageNode() StorageNode {
	if sn == nil {
		cfg, err := config.ReadConfig()
		if err != nil {
			log.Println("[init] GetStorageNode ReadConfig error:",err.Error())
			return nil
		}
		sn, err = node.NewStorageNode(cfg)
		if err != nil {
			log.Println("[init] GetStorageNode NewStorageNode error:",err.Error())
			return nil
		}
	}
	return sn
}
