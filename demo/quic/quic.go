package main

import (
	"context"
	"github.com/yottachain/YTDataNode/logger"
	"io"
	"io/ioutil"
	"os"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	inet "github.com/libp2p/go-libp2p-net"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	quictpt "github.com/libp2p/go-libp2p-quic-transport"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func main() {
	ctx := context.Background()
	host, err := libp2p.New(
		ctx,
		libp2p.ListenAddrStrings(os.Args[1]),
		libp2p.EnableRelay(circuit.OptHop, circuit.OptDiscovery),
		libp2p.Transport(quictpt.NewTransport),
		libp2p.DefaultTransports,
	)
	if err != nil {
		log.Println(err)
	}
	host.SetStreamHandler("test", func(stm inet.Stream) {
		log.Println("新链接建立")
		log.Printf("addrs:%s/p2p/%s\n", stm.Conn().RemoteMultiaddr().String(), stm.Conn().RemotePeer().Pretty())
		io.WriteString(stm, fmt.Sprintf("hello , this is %s\n", stm.Conn().LocalPeer().String()))
		stm.Close()
	})
	if len(os.Args) > 1 {
		for k, v := range os.Args {
			if k > 1 {
				addr, err := multiaddr.NewMultiaddr(v)
				info, err := peerstore.InfoFromP2pAddr(addr)
				if err != nil {
					panic(err)
				}
				host.Connect(ctx, *info)
				stm, err := host.NewStream(ctx, info.ID, "test")
				if err != nil {
					panic(err)
				}
				stm.Write([]byte("hello"))
				res, _ := ioutil.ReadAll(stm)
				log.Printf("%s\n", res)
				<-time.After(3 * time.Second)
			}
		}
	}
	log.Printf("初始化完成:\n")
	for k, v := range host.Addrs() {
		log.Printf("Addr[%d]:%s/p2p/%s\n", k, v.String(), host.ID().Pretty())
	}
	log.Printf("addrs:/p2p-circuit/p2p/%s\n", host.ID().Pretty())
	<-ctx.Done()
}
