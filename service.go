package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/yottachain/YTDataNode/host"

	"github.com/gogo/protobuf/proto"

	"github.com/yottachain/YTDataNode/message"

	"github.com/yottachain/YTFS"
)

type ytfsDisk *ytfs.YTFS

func (sn *storageNode) Service() {
	sn.host.HandleMessage("/node/0.0.1", func(data *host.MsgStream) {
		fmt.Println("创建新流：", data.Conn().RemoteMultiaddr().String()+"/p2p/"+data.Conn().RemotePeer().Pretty())
		info := sn.Host().Peerstore().PeerInfo(data.Conn().RemotePeer())
		for i, addr := range info.Addrs {
			fmt.Printf("远程地址:[%d]: %s", i, addr.String())
		}
		content := data.Content()
		msgTypeBuf := bytes.NewBuffer([]byte{})
		msgTypeBuf.Write(append([]byte{0, 0}, content[0:2]...))
		msgData := content[2:]
		var msgType int32
		binary.Read(msgTypeBuf, binary.BigEndian, &msgType)
		fmt.Println("收到消息", msgType)
		switch int32(msgType) {
		case message.MsgIDUploadShardRequest.Value():
			wh := WriteHandler{sn}
			data.SendMsgClose(wh.GetHandler(msgData))
		case message.MsgIDDownloadShardRequest.Value():
			dh := DownloadHandler{sn}
			data.SendMsgClose(dh.GetHandler(msgData))
		}

	})
	// Report(sn)
	Register(sn)
	go func() {
		for {
			Report(sn)
			time.Sleep(time.Second * 30)
		}
	}()
}

// Register 注册矿机
func Register(sn *storageNode) {
	var msg message.NodeRegReq
	msg.Nodeid = sn.Host().ID().Pretty()
	msg.Owner = sn.Host().ID().Pretty()
	msg.Addrs = sn.Host().AddrStrings()
	msg.MaxDataSpace = sn.YTFS().Meta().YtfsSize

	// if err := sn.Host().Connect("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{
	// 	"/ip4/172.21.0.13/tcp/9999",
	// 	"/ip4/152.136.11.202/tcp/9999",
	// }); err != nil {
	// 	fmt.Println("Connect bp fail", err)
	// }
	msgBytes, err := proto.Marshal(&msg)
	if err != nil {
		fmt.Println("Formate msg fail:", err)
	}
	stm, err := sn.host.NewMsgStream(context.Background(), "16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", "/node/0.0.1")
	if err != nil {
		fmt.Println("Create MsgStream fail:", err)
	} else {
		res, err := stm.SendMsgGetResponse(append(message.MsgIDNodeRegReq.Bytes(), msgBytes...))
		if err != nil {
			fmt.Println("Send reg msg fail:", err)
		} else {
			var resMsg message.NodeRegResp
			proto.Unmarshal(res[2:], &resMsg)
			fmt.Printf("Reg success, distribution space %d\n", resMsg.AssignedSpace)
		}
	}
}

// Report 上报状态
func Report(sn *storageNode) {
	// var msg message.StatusRepReq
	// for i, addr := range sn.Host().Addrs() {
	// 	fmt.Printf("addrs[%d]:%s\n", i, addr)
	// }

}
