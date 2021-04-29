package recover_test

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTHost"
	"github.com/yottachain/YTHost/option"
	"io/ioutil"
	"os"
	"testing"
)

var addr = "/ip4/127.0.0.1/tcp/9001/p2p/16Uiu2HAmC51JmypXUcH2HxCQoj7Tgu4JJVisxBsJzFB9twA7aJu9"

//var addr = "/ip4/49.233.89.168/tcp/9001/p2p/16Uiu2HAm6qug1Eb8RcHbnSWhsGnpxNMLUKf3Us8FaNCxN4ZvAGrh"
var liaddr, _ = multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9002")

func TestRecover(t *testing.T) {
	fi, _ := os.OpenFile("/Users/mac/Desktop/rcpackage.data", os.O_RDONLY, 0644)
	buf, _ := ioutil.ReadAll(fi)
	h, err := host.NewHost(option.ListenAddr(liaddr))
	fmt.Println(err, 0)
	ma, err := multiaddr.NewMultiaddr(addr)
	fmt.Println(err, 1)
	info, err := peer.AddrInfoFromP2pAddr(ma)
	fmt.Println(err, 2)
	clt, err := h.Connect(context.Background(), info.ID, info.Addrs)
	fmt.Println(err, 3)
	res, err := clt.SendMsgClose(context.Background(), message.MsgIDMultiTaskDescription.Value(), buf)
	fmt.Println(res, err, 4)
	select {}
}
