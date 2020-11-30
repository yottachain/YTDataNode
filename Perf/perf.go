package Perf

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTHost/client"
	"sync/atomic"
	"time"
)

var Sn storageNodeInterface.StorageNode

func TestMinerPerfHandler(data []byte) (res []byte, err error) {
	var successCount int64
	var errorCount int64

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(config.Gconfig.TTL))
	defer cancel()
	clt, err := Sn.Host().ClientStore().Get(ctx, pi.ID, pi.Addrs)
	if err != nil {
		return
	}

	outtime := time.Now().Add(time.Duration(task.TestTime) * time.Second)
	for {
		if outtime.Unix() < time.Now().Unix() {
			break
		}

		testerr := testOne(clt, requestbuf)
		if testerr == nil {
			atomic.AddInt64(&successCount, 1)
		} else {
			atomic.AddInt64(&errorCount, 1)
		}
	}

	// 构造返回消息
	var minerPerfResMsg message.TestMinerPerfTaskRes
	minerPerfResMsg.TargetMa = task.TargetMa
	minerPerfResMsg.TestType = task.TestType
	minerPerfResMsg.SuccessCount = successCount
	minerPerfResMsg.ErrorCount = errorCount
	res, err = proto.Marshal(&minerPerfResMsg)
	log.Println("[test] test task return", minerPerfResMsg)
	return
}

func testOne(clt *client.YTHostClient, requestbuf []byte) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(config.Gconfig.TTL))
	defer cancel()
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
	return nil
}

func GetBlock(data []byte) (res []byte, err error) {
	var msg message.TestGetBlock
	err = proto.Unmarshal(data, &msg)
	if err != nil {
		return
	}

	var resMsg message.TestGetBlockRes
	if len(msg.Msg) < 0 {
		resMsg.Msg = make([]byte, 16)
		rand.Read(resMsg.Msg)
	}

	res, err = proto.Marshal(&resMsg)
	return
}
