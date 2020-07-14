package main

import (
	"encoding/json"
	"flag"
//	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	ma "github.com/multiformats/go-multiaddr"
	"os"

	//	"github.com/yottachain/YTAschedule/logger"
	"github.com/yottachain/YTDataNode/message"
	host "github.com/yottachain/YTHost"
	"github.com/yottachain/YTHost/hostInterface"
	"golang.org/x/net/context"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"log"
)

var urlIP string
var port  string
var times uint
var timeout uint
var timeinterval uint
var loger *log.Logger
//type ID  string

type addrInfo struct {
	DnNum	  uint32
	NodeID    peer.ID
	Addrs 	  []ma.Multiaddr
}

func loginit() *log.Logger{
	file := "./" + time.Now().Format("20200701") + ".log"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if nil != err {
		panic(err)
	}
	loger := log.New(logFile, "[varify]", log.LstdFlags|log.Lshortfile|log.LUTC)
	return loger
}

func main(){
//	flag.StringVar(&urlIP,"ip","39.105.184.162","sn address")
	flag.StringVar(&urlIP,"ip","172.17.0.2","sn address")
	flag.StringVar(&port,"p","8082","sn serve port")
	flag.UintVar(&times,"n",10,"times need to do varify once for all datanode")
	flag.UintVar(&timeout,"t",600,"timeout(s) for connection")
	flag.UintVar(&timeinterval,"iv",10,"time interval for start varify")
	flag.Parse()

	var i uint32

	loger = loginit()

	hst,err := host.NewHost()
	if err != nil {
		loger.Println("error:",err)
	}

	for{
		dnList := GetAddrsBook(urlIP,port)
		for {
			<- time.After(time.Second * time.Duration(timeinterval))
			for _,item := range dnList {
				if item.DnNum % uint32(times) == i {
					SendCompareVarifyOrder(hst,item,timeout)
					loger.Println("item:",item)
				}
			}
			i++
			if i == uint32(times) {
				i = 0
				break
			}
		}
	}
//	fmt.Println("dnList:",dnList)
}

func SendCompareVarifyOrder(hst hostInterface.Host, info addrInfo, timeout uint) {
	var respMsg message.SelfVerifyResp

	ctx,cancel := context.WithTimeout(context.Background(), time.Second * time.Duration(timeout))
	defer cancel()

	clt,err := hst.ClientStore().Get(ctx,info.NodeID,info.Addrs)
	defer clt.Close()
	if err != nil {
		loger.Println("connet to server error:",err)
		return
	}

	if res, err := clt.SendMsg(context.Background(), message.MsgIDSelfVerifyReq.Value(), []byte("111111111111111")); err != nil {
		loger.Println("sendmsg error:",err)
	} else {
		loger.Println("res:",res)
		err = proto.Unmarshal(res[2:],&respMsg)
		if err != nil{
			loger.Println("err:",err,"respMsg:",respMsg)
			return
		}
		loger.Println("response nodeid:",respMsg.Id,"table idx:",respMsg.Numth,"err account:",respMsg.ErrNum)
	}
}

func GetAddrsBook(snAddr, port string)(res []addrInfo){
	url := "http://"+snAddr+":"+port+"/active_nodes"
	loger.Println("url:",url)
	resp,err:=http.Get(url)
//	resp,err:=http.Get("http://39.105.184.162:8082/active_nodes")
	if err != nil {
		loger.Println(err.Error())
		return nil
	}

	buf,err:=ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	type peerInfo struct {
		IP string `json:"ip"`
		ID string `json:"id"`
		NodeID string `json:"nodeid"`
	}

	var list =make([]peerInfo,0)

	err = json.Unmarshal(buf,&list)
	if err != nil{
		loger.Println("Unmarshal error:",err)
	}

	if err != nil {
		loger.Println(err.Error())
		return nil
	}

	for _,item:= range list {
		id,_ := strconv.ParseUint(item.ID,10,32)
		nodeid,err := peer.Decode(item.NodeID)
		if err != nil {
			continue
		}

		ma,err := multiaddr.NewMultiaddr(item.IP)
		if err != nil {
			continue
		}
		//log.Println("NodeID:",item.NodeID,"item.IP",item.IP,"item.ID",item.ID)
		//log.Println("nodeid:",nodeid,"ma:",ma,"dnnum:",item.ID)

		res = append(res,addrInfo{
			uint32(id),
			nodeid,
			[]multiaddr.Multiaddr{ma},
		})
	}

	return
}
