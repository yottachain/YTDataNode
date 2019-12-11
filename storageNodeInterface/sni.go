package storageNodeInterface

import (
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/host"
	. "github.com/yottachain/YTDataNode/runtimeStatus"
	ytfs "github.com/yottachain/YTFS"
)

// Owner 归属信息
type Owner struct {
	ID       string
	BuySpace uint64
	HDD      uint64
}

// StorageNode 存储节点接口
type StorageNode interface {
	Addrs() []string
	Host() *host.Host
	YTFS() *ytfs.YTFS
	GetBP() int
	Service()
	Config() *config.Config
	Runtime() RuntimeStatus
	Owner() *Owner
	SendBPMsg(index int, data []byte) ([]byte, error)
}
