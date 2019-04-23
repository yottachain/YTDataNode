package node

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/yottachain/P2PHost"
	"github.com/yottachain/YTDataNode/message"

	"github.com/yottachain/YTFS"
)

type ytfsDisk *ytfs.YTFS

func (sn *storageNode) Service() {
	sn.host.RegisterHandler("message", func(data host.Msg) []byte {
		msgTypeBuf := bytes.NewBuffer(data.Content[0:2])
		msgData := data.Content[2:]
		var msgType int
		binary.Read(msgTypeBuf, binary.BigEndian, &msgType)
		fmt.Println("收到消息", msgType)
		switch msgType {
		case message.MsgIDUploadShardRequest:
			wh := WriteHandler{sn}
			return wh.GetHandler(msgData)
		case message.MsgIDDownloadShardRequest:
			dh := DownloadHandler{sn}
			return dh.GetHandler(msgData)
		}
		return nil
	})
}
