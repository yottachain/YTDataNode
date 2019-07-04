package service

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/yottachain/YTDataNode/host"
	"github.com/yottachain/YTDataNode/message"
)

type handlerFunc func(msgData []byte) []byte

// HandlerMap 消息处理器列表
type HandlerMap map[int32]handlerFunc

// HandleMsgService 消息处理管理
type HandleMsgService struct {
	handler map[string]HandlerMap
	host    *host.Host
}

// RegitsterHandler 注册消息处理器
func (hm *HandleMsgService) RegitsterHandler(protocol string, msgType int32, handler handlerFunc) {
	if hm.handler == nil {
		hm.handler = make(map[string]HandlerMap)
	}
	if hm.handler[protocol] == nil {
		hm.handler[protocol] = make(HandlerMap)
	}
	if hm.handler[protocol][msgType] == nil {
		hm.handler[protocol][msgType] = handler
	}
}

// Service 启动服务
func (hm *HandleMsgService) Service() {
	for protocol, hmp := range hm.handler {
		hm.host.HandleMessage(protocol, func(data *host.MsgStream) {
			log.Println("创建新流：", data.Conn().RemoteMultiaddr().String()+"/p2p/"+data.Conn().RemotePeer().Pretty())
			info := hm.host.Peerstore().PeerInfo(data.Conn().RemotePeer())

			for i, addr := range info.Addrs {
				log.Printf("远程地址:[%d]: %s", i, addr.String())
			}
			content := data.Content()
			msgType, msgData, err := hm.ParseMsg(content)
			if err != nil {
				data.SendMsgClose(append(message.MsgIDDownloadShardResponse.Bytes(), []byte{102}...))
				log.Println(fmt.Sprintf("%c[0;0;31m content len error : %d%c[0m\n", 0x1B, len(content), 0x1B))
			} else {
				if hmp[msgType] != nil {
					data.SendMsgClose(hmp[msgType](msgData))
				}
			}
		})
	}
}

// ParseMsg 启动服务
func (hm *HandleMsgService) ParseMsg(content []byte) (int32, []byte, error) {
	if len(content) < 2 {
		return 0, nil, fmt.Errorf("msg error")
	}
	msgTypeBuf := bytes.NewBuffer([]byte{})
	msgTypeBuf.Write(append([]byte{0, 0}, content[0:2]...))
	var msgType int32
	binary.Read(msgTypeBuf, binary.BigEndian, &msgType)
	return msgType, content[2:], nil
}

// NewHandleMsgService 创建消息处理服务
func NewHandleMsgService(hst *host.Host) *HandleMsgService {
	hms := new(HandleMsgService)
	hms.host = hst
	return hms
}
