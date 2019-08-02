package main

import (
	"context"
	"encoding/gob"
	"log"
	"io/ioutil"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/message"

	peerstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p"
)

func main() {
	host, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9002"))
	if err != nil {
		log.Println("err ", err)
	}
	ma, err := multiaddr.NewMultiaddr(os.Args[1])
	if err != nil {
		log.Println("错误 ", err)
	}
	info, err := peerstore.InfoFromP2pAddr(ma)
	if err != nil {
		log.Println("错误3 ", err)
	}
	host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)
	stm, err := host.NewStream(context.Background(), info.ID, "/node/0.0.1")
	if err != nil {
		log.Println("错误 4", err)
	}
	ed := gob.NewEncoder(stm)
	var msg message.StringMsg
	msg.Msg = "ping"
	buf, err := proto.Marshal(&msg)
	if err != nil {
		log.Println("错误 5", err)
	}
	ed.Encode(append(message.MsgIDString.Bytes(), buf...))
	res, _ := ioutil.ReadAll(stm)
	log.Printf("%s\n", res)
}
