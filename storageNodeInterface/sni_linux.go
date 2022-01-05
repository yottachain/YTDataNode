package storageNodeInterface

import (
	"github.com/tecbot/gorocksdb"
	"github.com/yottachain/YTDataNode/config"
	. "github.com/yottachain/YTDataNode/runtimeStatus"
	ytfs "github.com/yottachain/YTFS"
	. "github.com/yottachain/YTHost/interface"
)

// Owner 归属信息
type Owner struct {
	ID       string
	BuySpace uint64
	HDD      uint64
}

type CompDB struct {
	Db  *gorocksdb.DB
	Ro  *gorocksdb.ReadOptions
	Wo  *gorocksdb.WriteOptions
}

// StorageNode 存储节点接口
type StorageNode interface {
	Addrs() []string
	Host() Host
	YTFS() *ytfs.YTFS
	GetBP() int
	Service()
	Config() *config.Config
	Runtime() RuntimeStatus
	Owner() *Owner
	SendBPMsg(index int, id int32, data []byte) ([]byte, error)
	GetCompareDb() *CompDB
}
