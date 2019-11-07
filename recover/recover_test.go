package recover

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/message"
	"io/ioutil"
	"os"
	"testing"
)

var addr = "/ip4/127.0.0.1/tcp/9001/p2p/16Uiu2HAmCesehUznuW6moZgPAWoJrryXDjbX4gbqn5Zet7f2db2e"

func TestRecover(t *testing.T) {
	fi, _ := os.OpenFile("/Users/mac/go/src/github.com/yottachain/YTDataNode/recover/test.dat", os.O_RDONLY, 0644)
	buf, _ := ioutil.ReadAll(fi)
	h, _ := libp2p.New(context.Background())
	ma, _ := multiaddr.NewMultiaddr(addr)
	info, _ := peer.AddrInfoFromP2pAddr(ma)
	err := h.Connect(context.Background(), *info)
	fmt.Println(err)
	stm, err := h.NewStream(context.Background(), info.ID, "/node/0.0.2")
	fmt.Println("err", err, len(buf))
	ee := gob.NewEncoder(stm)
	ee.Encode(append(message.MsgIDMultiTaskDescription.Bytes(), buf[:]...))
	select {}
}
