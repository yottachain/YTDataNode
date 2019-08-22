package main

import (
	"context"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"github.com/yottachain/YTDataNode/logger"
	"io/ioutil"
	"os"

	"github.com/yottachain/YTDataNode/message"

	peerstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p"
)

var que chan int

func main() {

	que = make(chan int, 1000)

	for i := 0; i < 1000; i++ {
		que <- i
	}
	for {
		go sendMsg(<-que)
	}
}

func sendMsg(i int) {
	defer func(k int) {
		que <- k
	}(i)
	host, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", i+9002)))
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
	var data []byte
	data = make([]byte, 16*1024)
	rand.Read(data)
	if err != nil {
		log.Println("错误 5", err)
	}
	ed.Encode(append(message.MsgIDString.Bytes(), data...))
	res, _ := ioutil.ReadAll(stm)
	log.Printf("%s\n", res)
}
