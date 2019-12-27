package recover_test

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTHost"
	"io/ioutil"
	"os"
	"testing"
)

var addr = "/ip4/127.0.0.1/tcp/9002/p2p/16Uiu2HAkva8EVP6Maag6GmyR2fsxsZrwqB8rawJJ2aKv5hhRTnsy"

//var addr = "/ip4/49.233.89.168/tcp/9001/p2p/16Uiu2HAm6qug1Eb8RcHbnSWhsGnpxNMLUKf3Us8FaNCxN4ZvAGrh"

func TestRecover(t *testing.T) {
	fi, _ := os.OpenFile("/Users/mac/go/src/github.com/yottachain/YTDataNode/recover/rcpackage.data", os.O_RDONLY, 0644)
	buf, _ := ioutil.ReadAll(fi)
	h, _ := host.NewHost()
	ma, _ := multiaddr.NewMultiaddr(addr)
	info, _ := peer.AddrInfoFromP2pAddr(ma)
	clt, err := h.Connect(context.Background(), info.ID, info.Addrs)
	fmt.Println(err)
	res, err := clt.SendMsgClose(context.Background(), message.MsgIDMultiTaskDescription.Value(), buf)
	fmt.Println(res, err)
	select {}
}
