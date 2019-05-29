package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
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
			time.Sleep(time.Second * 60)
		}
	}()
}

// Register 注册矿机
func Register(sn *storageNode) {
	var msg message.NodeRegReq
	msg.Nodeid = sn.Host().ID().Pretty()
	msg.Owner = os.Getenv("owner")
	fmt.Println("owner:", msg.Owner)
	msg.Addrs = sn.Addrs()
	msg.MaxDataSpace = sn.YTFS().Meta().YtfsSize

	bp := sn.Config().BPList[sn.GetBP()]
	if err := sn.Host().ConnectAddrStrings(bp.ID, bp.Addrs); err != nil {
		fmt.Println("Connect bp fail", err)
	}
	msgBytes, err := proto.Marshal(&msg)
	if err != nil {
		fmt.Println("Formate msg fail:", err)
	}
	fmt.Println("sn index:", sn.GetBP())
	stm, err := sn.host.NewMsgStream(context.Background(), bp.ID, "/node/0.0.1")
	if err != nil {
		fmt.Println("Create MsgStream fail:", err)
	} else {
		res, err := stm.SendMsgGetResponse(append(message.MsgIDNodeRegReq.Bytes(), msgBytes...))
		if err != nil {
			fmt.Println("Send reg msg fail:", err)
		} else {
			var resMsg message.NodeRegResp
			proto.Unmarshal(res[2:], &resMsg)
			sn.Config().IndexID = resMsg.Id
			sn.Config().Save()
			sn.owner.BuySpace = resMsg.AssignedSpace
			fmt.Printf("id %d, Reg success, distribution space %d\n", resMsg.Id, resMsg.AssignedSpace)
		}
	}
}

// Report 上报状态
func Report(sn *storageNode) {
	var msg message.StatusRepReq
	bp := sn.Config().BPList[sn.GetBP()]
	msg.Addrs = sn.Addrs()
	msg.Cpu = sn.Runtime().AvCPU
	msg.Memory = sn.Runtime().Mem
	msg.Id = sn.Config().IndexID
	msg.MaxDataSpace = sn.YTFS().Meta().YtfsSize / uint64(sn.YTFS().Meta().DataBlockSize)
	msg.UsedSpace = sn.YTFS().Len() / uint64(sn.YTFS().Meta().DataBlockSize)
	resData, err := proto.Marshal(&msg)
	fmt.Printf("cpu:%d%% mem:%d%% max-space: %d block\n", msg.Cpu, msg.Memory, msg.MaxDataSpace)
	if err != nil {
		fmt.Println("send report msg fail:", err)
	}
	if err := sn.Host().ConnectAddrStrings(bp.ID, bp.Addrs); err != nil {
		fmt.Println("Connect bp fail", err)
	}
	stm, err := sn.host.NewMsgStream(context.Background(), bp.ID, "/node/0.0.1")
	if err != nil {
		fmt.Println("Create MsgStream fail:", err)
	} else {
		res, err := stm.SendMsgGetResponse(append(message.MsgIDStatusRepReq.Bytes(), resData...))
		if err != nil {
			fmt.Println("Send report msg fail:", err)
		} else {
			var resMsg message.StatusRepResp
			proto.Unmarshal(res[2:], &resMsg)
			sn.owner.BuySpace = resMsg.ProductiveSpace
			fmt.Printf("report info success: %d\n", resMsg.ProductiveSpace)
		}
	}

}
