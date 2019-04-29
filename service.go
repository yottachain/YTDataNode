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
	sn.host.RegisterHandler("/node/0.0.1", func(data host.Msg) []byte {
		msgTypeBuf := bytes.NewBuffer([]byte{})
		msgTypeBuf.Write(append([]byte{0, 0}, data.Content[0:2]...))
		msgData := data.Content[2:]
		fmt.Println(data.Content[0:2])
		var msgType int32
		binary.Read(msgTypeBuf, binary.BigEndian, &msgType)
		fmt.Println("收到消息", msgType)
		switch int32(msgType) {
		case message.MsgIDUploadShardRequest.Value():
			wh := WriteHandler{sn}
			return wh.GetHandler(msgData)
		case message.MsgIDDownloadShardRequest.Value():
			dh := DownloadHandler{sn}
			return dh.GetHandler(msgData)
		}
		return nil
	})
}
