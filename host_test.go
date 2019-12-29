package node_test

import (
	"fmt"
	host "github.com/graydream/YTHost"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/message"
	"golang.org/x/net/context"
	"testing"
)

func TestSendMsg(t *testing.T){
	hst,err := host.NewHost_TEST()
	if err != nil {
		t.Fatal(err)
	}
	mastr:= "/ip4/0.0.0.0/tcp/9001/p2p/16Uiu2HAmEpzQymSsuEX2AkBqqxz2Ajq3TJFdTkMezQCkZkvvq7NB"

	ma,err := multiaddr.NewMultiaddr(mastr)
	if err != nil {
		t.Fatal(err)
	}

	pi,err:=peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		t.Fatal(err)
	}

	clt,err:=hst.Connect(context.Background(),pi.ID,pi.Addrs)
	if err != nil {
		t.Fatal(err)
	}

	stringtest:="xiaojm  this is a test MsgIDListDNIResp"
	testInfo:=[]byte(stringtest)
    for i:=0; i<10;i++ {
		clt.SendMsg(context.Background(), message.MsgIDListDNIReq.Value(), testInfo)
		fmt.Println("here:",string(testInfo))
	}
}
