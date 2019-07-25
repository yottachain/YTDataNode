package test

import (
	"context"
	"fmt"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/host"
	"github.com/yottachain/YTDataNode/util"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

const (
	m1 = "/ip4/0.0.0.0/tcp/9001"
	m2 = "/ip4/0.0.0.0/tcp/9003"
)

func TestSendMsg(t *testing.T) {
	h := host.NewP2PHost()
	h.Daemon(context.Background(), m2)
	file, err := os.OpenFile(path.Join(util.GetYTFSPath(), "buf"), os.O_RDWR, 0666)
	buf, err := ioutil.ReadAll(file)
	fmt.Println(err)
	ma, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/9001/p2p/16Uiu2HAmFAuu9422V1tqnDnqQ6DjWWPhXYpwzMKvZ38C5PCFZXyw")
	pi, err := pstore.InfoFromP2pAddr(ma)
	h.Connect(context.Background(), *pi)
	res, err := h.SendMsg("16Uiu2HAmFAuu9422V1tqnDnqQ6DjWWPhXYpwzMKvZ38C5PCFZXyw", "/node/0.0.1", buf)
	t.Log(res, err)
	<-time.After(10 * time.Second)
}

func TestAddPeer(t *testing.T) {
}
