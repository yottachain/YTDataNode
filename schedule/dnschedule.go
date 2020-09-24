package main

import (
	"encoding/json"
	"flag"
	"os"
	"strconv"

	//	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/message"
	host "github.com/yottachain/YTHost"
	hostInterface "github.com/yottachain/YTHost/interface"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)


var urlIP string
var port  string
var codeType string
var times uint
var timeout uint
var timeinterval uint
var startDN uint

var loger *log.Logger
//type ID  string

type gPeerInfo struct{
	IP []string `json:"ip"`
	ID string `json:"id"`
	NodeID string `json:"nodeid"`
}

type jPeerInfo struct {
	IP string `json:"ip"`
	ID string `json:"id"`
	NodeID string `json:"nodeid"`
}

type addrInfo struct {
	DnNum	  uint32
	NodeID    peer.ID
	Addrs 	  []multiaddr.Multiaddr
}

func loginit() *log.Logger{
	file := "./" + time.Now().Format("20200701") + ".log"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if nil != err {
		panic(err)
	}
	loger := log.New(logFile, "[verify]", log.LstdFlags|log.Lshortfile|log.LUTC)
	return loger
}

func main(){
//	flag.StringVar(&urlIP,"ip","39.105.184.162","sn address")
	flag.StringVar(&codeType,"code","go","sn code type(go or java)")
	flag.StringVar(&urlIP,"ip","172.17.0.2","sn address")
	flag.StringVar(&port,"p","8082","sn serve port")
	flag.UintVar(&startDN,"sd",6500,"the begin datanode id to start verify")
	flag.UintVar(&times,"n",10,"times need to do varify once for all datanode")
	flag.UintVar(&timeout,"t",20,"timeout(s) for connection")
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
				if item.DnNum < uint32(startDN){
					continue
				}

				if item.DnNum % uint32(times) == i {
					err = SendCompareVarifyOrder(hst,item,timeout)
					if err != nil{
						log.Println("error", err)
						continue
					}
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

func SendCompareVarifyOrder(hst hostInterface.Host, info addrInfo, timeout uint) error{
	var respMsg message.SelfVerifyResp

	ctx,cancel := context.WithTimeout(context.Background(), time.Second * time.Duration(timeout))
	defer cancel()

	clt,err := hst.ClientStore().Get(ctx,info.NodeID,info.Addrs)
	if clt != nil{
		defer clt.Close()
	}

	if err != nil {
		loger.Println("connet to server error:",err)
		return err
	}

	if res, err := clt.SendMsg(context.Background(), message.MsgIDSelfVerifyReq.Value(), []byte("111111111111111")); err != nil {
		loger.Println("sendmsg error:",err)
	} else {
//		loger.Println("res:",res)
		err = proto.Unmarshal(res[2:],&respMsg)
		if err != nil{
			loger.Println("err:",err,"respMsg:",respMsg)
			return err
		}
		loger.Println("response nodeid:",respMsg.Id,"table idx:",respMsg.Numth,"err account:",respMsg.ErrNum)
	}
	return err
}

func getGoDnList(list []gPeerInfo) (res []addrInfo){
	for _,item:= range list {
		id,_ := strconv.ParseUint(item.ID,10,32)
		nodeid,err := peer.Decode(item.NodeID)
		if err != nil {
			continue
		}

		ma,err := multiaddr.NewMultiaddr(item.IP[0])
		if err != nil {
			continue
		}

		res = append(res,addrInfo{
			uint32(id),
			nodeid,
			[]multiaddr.Multiaddr{ma},
		})
	}
	return res
}

func getJvDnList(list []jPeerInfo) (res []addrInfo){
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

		res = append(res,addrInfo{
			uint32(id),
			nodeid,
			[]multiaddr.Multiaddr{ma},
		})
	}
	return res
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

	if codeType == "java" {
		var jlist = make([]jPeerInfo,0)
		err = json.Unmarshal(buf,&jlist)
		if err != nil {
			loger.Println(err.Error())
			panic(err)
		}
		res = getJvDnList(jlist)
	}else{
		var glist = make([]gPeerInfo,0)
		err = json.Unmarshal(buf,&glist)
		if err != nil {
			loger.Println(err.Error())
			panic(err)
		}
		res = getGoDnList(glist)
	}
	return
}
