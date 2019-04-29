package p2p

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	"github.com/libp2p/go-libp2p-host"
	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/mr-tron/base58"
)

type Host struct {
	host.Host
	bootstrapList []*peerstore.PeerInfo
}

// NewP2PHost 创建p2p节点
func NewP2PHost(pkstring string) (host.Host, error) {
	ctx := context.Background()
	pkbytes, err := base58.Decode(pkstring)
	if err != nil {
		return nil, fmt.Errorf("Bad private key string")
	}
	pk, err := ci.UnmarshalSecp256k1PrivateKey(pkbytes[1:33])
	if err != nil {
		return nil, fmt.Errorf("Bad format of private key")
	}
	return libp2p.New(
		ctx,
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/9001"),
		libp2p.EnableRelay(circuit.OptHop, circuit.OptDiscovery),
	)
}
