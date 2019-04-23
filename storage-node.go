package node

import (
	"fmt"
	"yottachain/ytfs-util"

	"github.com/yottachain/P2PHost"

	"github.com/libp2p/go-libp2p-peer"

	ci "github.com/libp2p/go-libp2p-crypto"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTFS"
	ytfsOpts "github.com/yottachain/YTFS/opt"
)

// StorageNode 存储节点接口
type StorageNode interface {
	Host() host.Host
	YTFS() *ytfs.YTFS
	GetBP(bpid int32) peer.ID
	Service()
}

type storageNode struct {
	host   host.Host
	ytfs   *ytfs.YTFS
	bplist []peer.ID
}

func (sn *storageNode) Host() host.Host {
	return sn.host
}

func (sn *storageNode) YTFS() *ytfs.YTFS {
	return sn.ytfs
}

func (sn *storageNode) GetBP(bpid int32) peer.ID {
	return sn.bplist[bpid]
}

// NewStorageNode 创建存储节点
func NewStorageNode(pkstring string) (StorageNode, error) {

	pkbytes, err := base58.Decode(pkstring)
	if err != nil {
		return nil, fmt.Errorf("Bad private key string")
	}
	pk, err := ci.UnmarshalSecp256k1PrivateKey(pkbytes[1:33])
	if err != nil {
		return nil, fmt.Errorf("Bad format of private key")
	}

	sn := &storageNode{}
	h, err := host.NewHost(host.ListenAddrStrings("/ip4/0.0.0.0/tcp/9001"), pk)

	sn.host = h
	opts := ytfsOpts.DefaultOptions()
	yp := util.GetYTFSPath()
	for index, storage := range opts.Storages {
		storage.StorageName = fmt.Sprintf("%s/storage-%d", yp, index)
		opts.Storages[index] = storage
	}
	ys, err := ytfs.Open(yp, opts)
	if err != nil {
		return nil, fmt.Errorf("YTFS storage init faile")
	}
	sn.ytfs = ys
	if err != nil {
		return nil, err
	}
	return sn, nil
}
