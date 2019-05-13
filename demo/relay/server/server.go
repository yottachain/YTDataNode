package main

import (
	"context"
	"fmt"

	libp2p "github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	inet "github.com/libp2p/go-libp2p-net"
)

func main() {
	ctx := context.Background()
	host, err := libp2p.New(
		ctx,
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9003"),
		libp2p.EnableRelay(circuit.OptHop),
	)
	if err != nil {
		fmt.Println(err)
	}
	host.SetStreamHandler("test", func(stm inet.Stream) {
		fmt.Println("新链接建立")
		fmt.Printf("addrs:%s/p2p/%s\n", stm.Conn().RemoteMultiaddr().String(), stm.Conn().RemotePeer().Pretty())
	})
	fmt.Printf("初始化完成:\n")
	for k, v := range host.Addrs() {
		fmt.Printf("Addr[%d]:%s/p2p/%s\n", k, v.String(), host.ID().Pretty())
	}
	<-ctx.Done()
}
