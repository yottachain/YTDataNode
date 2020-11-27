package Perf

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/storageNodeInterface"
	"time"
)

var Sn storageNodeInterface.StorageNode

func TestMinerPerfHandler(data []byte) (res []byte, err error) {
	var task message.TestMinerPerfTask
	err = proto.UnmarshalMerge(data, &task)
	if err != nil {
		return
	}

	// 构造请求
	var requestMsg message.TestGetBlock
	if task.TestType == 0 {
		requestMsg.Msg = make([]byte, 16)
		rand.Read(requestMsg.Msg)
	}
	requestMsg.Timestamp = time.Now().Unix()
	requestbuf, err := proto.Marshal(&requestMsg)
	if err != nil {
		return
	}

	var pi = &peer.AddrInfo{}
	// 解系地址
	for _, addr := range task.TargetMa {
		ma, err2 := multiaddr.NewMultiaddr(addr)
		if err2 != nil {
			continue
		}
		i, err2 := peer.AddrInfoFromP2pAddr(ma)
		if err2 != nil {
			continue
		}
		pi.ID = i.ID
		pi.Addrs = append(pi.Addrs, i.Addrs...)
	}

	if len(pi.Addrs) <= 0 {
		err = fmt.Errorf("no addr")
		return
	}

	// 建立连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	clt, err := Sn.Host().ClientStore().Get(ctx, pi.ID, pi.Addrs)

	// 发送消息
	resbuf, err := clt.SendMsg(ctx, message.MsgIDTestGetBlock.Value(), requestbuf)
	if err != nil {
		return
	}

	// 解析回复消息
	var resMsg message.TestGetBlockRes
	err = proto.Unmarshal(resbuf, &resMsg)
	if err != nil {
		return
	}

	// 构造返回消息
	var minerPerfResMsg message.TestMinerPerfTaskRes
	minerPerfResMsg.TargetMa = task.TargetMa
	minerPerfResMsg.TestType = task.TestType
	minerPerfResMsg.Latency = time.Now().UnixNano() - resMsg.Timestamp
	res, err = proto.Marshal(&minerPerfResMsg)
	log.Println("[test] test task return", minerPerfResMsg)
	return
}

func GetBlock(data []byte) (res []byte, err error) {
	var msg message.TestGetBlock
	err = proto.Unmarshal(data, &msg)
	if err != nil {
		return
	}

	var resMsg message.TestGetBlockRes
	if len(msg.Msg) > 0 {
		resMsg.Timestamp = msg.Timestamp
	} else {
		resMsg.Msg = make([]byte, 16)
		rand.Read(resMsg.Msg)
		resMsg.Timestamp = time.Now().UnixNano()
	}

	res, err = proto.Marshal(&resMsg)
	return
}
