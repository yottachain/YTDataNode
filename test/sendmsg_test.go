package test

import (
	"fmt"
	"testing"

	node "github.com/yottachain/YTDataNode"
)

const (
	snpk   = "5JB7HxBrsDYtMrjJUsJ5WLdJRK3KJUrPHD6eBphVYPrXoxcqLtc"
	bpid   = "16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi"
	bpaddr = "/ip4/152.136.11.202/tcp/9999"
)

func TestSendMsgToBP(t *testing.T) {
	sn, err := node.NewStorageNode("5JB7HxBrsDYtMrjJUsJ5WLdJRK3KJUrPHD6eBphVYPrXoxcqLtc")
	if err != nil {
		t.Error("init node error")
	}
	err = sn.Host().ConnectAddrString(bpaddr + "/p2p/" + bpid)
	if err != nil {
		t.Error(err)
	}
	res, err := sn.Host().SendMsg(bpid, "/bpnode/0.0.1", []byte("test"))

	if err != nil {
		t.Error("res error:", err)
	}
	t.Log(res)
	pk := sn.Host().PrivKey()
	fmt.Println(pk.Bytes())
	// id, _ := peer.IDB58Decode("16Uiu2HAm5hqd85Hzpvvg4BfVBVfAsXPaRMj9YNhwkkGnD2Qiqxn9")
	// t.Log(pk)
	// host, err := libp2p.New(context.Background())
	// if err != nil {
	// 	t.Error(err)
	// }
	// addr, _ := multiaddr.NewMultiaddr("/ip4/152.136.11.202/tcp/9999/p2p/16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi")
	// info, err := peerstore.InfoFromP2pAddr(addr)
	// err = host.Connect(context.Background(), *info)
	// if err != nil {
	// 	t.Error(err)
	// }
	// select {}
}
