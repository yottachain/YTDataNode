package node

import (
	"fmt"
	"yottachain/ytfs-util"

	"github.com/yottachain/YTDataNode/config"

	"github.com/yottachain/YTDataNode/host"

	// "github.com/yottachain/P2PHost"

	"github.com/libp2p/go-libp2p-peer"

	"github.com/yottachain/YTFS"
)

// StorageNode 存储节点接口
type StorageNode interface {
	Host() *host.Host
	YTFS() *ytfs.YTFS
	GetBP(bpid int32) peer.ID
	Service()
	Config() *config.Config
}

type storageNode struct {
	host   *host.Host
	ytfs   *ytfs.YTFS
	bplist []peer.ID
	config *config.Config
}

func (sn *storageNode) Host() *host.Host {
	return sn.host
}
func (sn *storageNode) Config() *config.Config {
	return sn.config
}

func (sn *storageNode) YTFS() *ytfs.YTFS {
	return sn.ytfs
}

func (sn *storageNode) GetBP(bpid int32) peer.ID {
	return sn.bplist[bpid]
}

// NewStorageNode 创建存储节点
func NewStorageNode(cfg *config.Config) (StorageNode, error) {
	// pkbytes, err := base58.Decode(pkstring)
	// if err != nil {
	// 	return nil, fmt.Errorf("Bad private key string")
	// }
	// pk, err := ci.UnmarshalSecp256k1PrivateKey(pkbytes[1:33])
	// if err != nil {
	// 	return nil, fmt.Errorf("Bad format of private key")
	// }

	sn := &storageNode{}
	sn.config = cfg
	// h, err := host.NewHost(host.ListenAddrStrings("/ip4/0.0.0.0/tcp/9001"), pk)

	sn.host = host.NewP2PHost()
	sn.host.SetPrivKey(sn.config.PrivKey())
	yp := util.GetYTFSPath()
	ys, err := ytfs.Open(yp, cfg.Options)
	if err != nil {
		return nil, fmt.Errorf("YTFS storage init faile")
	}
	sn.ytfs = ys
	if err != nil {
		return nil, err
	}
	sn.bplist = []peer.ID{
		"16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi",
	}
	return sn, nil
}
