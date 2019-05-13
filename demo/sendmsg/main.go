package main

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/yottachain/YTDataNode/message"

	"github.com/libp2p/go-libp2p-peerstore"
	"github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p"
)

func main() {
	host, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9001"))
	if err != nil {
		fmt.Println("err ", err)
	}
	ma, err := multiaddr.NewMultiaddr("/ip4/152.136.13.254/tcp/9001/p2p/16Uiu2HAm5hqd85Hzpvvg4BfVBVfAsXPaRMj9YNhwkkGnD2Qiqxn9")
	if err != nil {
		fmt.Println("错误 ", err)
	}
	info, err := peerstore.InfoFromP2pAddr(ma)
	if err != nil {
		fmt.Println("错误3 ", err)
	}
	host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)
	stm, err := host.NewStream(context.Background(), info.ID, "/node/0.0.1")
	if err != nil {
		fmt.Println("错误 4", err)
	}
	ed := gob.NewEncoder(stm)
	var msg message.UploadShardRequest
	buf, err := proto.Marshal(&msg)
	if err != nil {
		fmt.Println("错误 5", err)
	}
	ed.Encode(append(message.MsgIDUploadShardRequest.Bytes(), buf...))
}
