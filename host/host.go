package host

import (
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"

	"github.com/libp2p/go-libp2p-peer"
	"github.com/mr-tron/base58"
	multiaddr "github.com/multiformats/go-multiaddr"

	libp2p "github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	ci "github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	"github.com/libp2p/go-libp2p-peerstore"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

// Host 节点host
type Host struct {
	host.Host
	superNodes []*peerstore.PeerInfo
	privKey    ci.PrivKey
}

// NewP2PHost 创建p2p节点
func NewP2PHost() *Host {
	var h Host
	return &h
}

// PrivKey 私钥
func (h *Host) PrivKey() ci.PrivKey {
	return h.privKey
}

// SetPrivKey 设置私钥
func (h *Host) SetPrivKey(pkstring string) error {
	pkbytes, err := base58.Decode(pkstring)
	if err != nil {
		return fmt.Errorf("Bad private key string")
	}
	pk, err := ci.UnmarshalSecp256k1PrivateKey(pkbytes[1:33])
	if err != nil {
		return fmt.Errorf("Bad format of private key")
	}
	h.privKey = pk
	return nil
}

// SetSuperNodes 设置启动节点
func (h *Host) SetSuperNodes(nodes []string) error {
	for _, v := range nodes {
		node, err := InfoFromAddrString(v)
		if err != nil {
			return err
		}
		h.superNodes = append(h.superNodes, node)
	}
	return nil
}

// AddrStrings 返回地址字符串数组形式
func (h *Host) AddrStrings() []string {
	maddrs := h.Addrs()
	addrs := make([]string, len(maddrs))
	for k, ma := range maddrs {
		addrs[k] = ma.String()
	}
	return addrs
}

// ConnectAddrStrings 添加地址
func (h *Host) ConnectAddrStrings(id string, addrs []string) error {
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for k, addr := range addrs {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return fmt.Errorf("Addrstring parse fail:%s", addr)
		}
		maddrs[k] = maddr
	}
	pid, err := peer.IDB58Decode(id)
	if err != nil {
		return fmt.Errorf("ID formate fail：%s", err)
	}

	return h.Connect(context.Background(), peerstore.PeerInfo{
		ID:    pid,
		Addrs: maddrs,
	})
}

// Daemon 启动host节点
func (h *Host) Daemon(ctx context.Context, addr string) error {
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(addr),
		libp2p.NATPortMap(),
		libp2p.EnableRelay(circuit.OptHop),
	}
	if h.privKey != nil {
		opts = append(opts, libp2p.Identity(h.privKey))
	}
	host, err := libp2p.New(
		ctx,
		opts...,
	)
	if err != nil {
		fmt.Println("p2p host init fail:", err)
		return err
	}
	h.Host = host
	for _, v := range h.superNodes {
		h.Peerstore().AddAddrs(v.ID, v.Addrs, peerstore.AddressTTL)
	}
	return nil
}

// GetSuperNode 获取超级节点
func (h Host) GetSuperNode(index uint) *peerstore.PeerInfo {
	return h.superNodes[index]
}

// MsgStream 消息流
type MsgStream struct {
	inet.Stream
	ed *gob.Encoder
	dd *gob.Decoder
}

type msgHandler func(msg *MsgStream)

// Content 消息内容
func (m *MsgStream) Content() []byte {
	var content []byte
	m.dd.Decode(&content)
	return content
}

// SendMsg 发送消息
func (m *MsgStream) SendMsg(msgData []byte) {
	ed := gob.NewEncoder(m)
	ed.Encode(msgData)
}

// SendMsgClose 发送消息并且关闭通道
func (m *MsgStream) SendMsgClose(msgData []byte) {
	defer m.Close()
	m.Write(msgData)
}

// SendMsgGetResponse 发送消息并且获取返回
func (m *MsgStream) SendMsgGetResponse(msgData []byte) ([]byte, error) {
	m.SendMsg(msgData)
	fmt.Println("send msg success")
	return ioutil.ReadAll(m)
}

// HandleMessage 处理消息
func (h *Host) HandleMessage(protocolID string, handler msgHandler) {
	h.SetStreamHandler(protocol.ID(protocolID), func(stream inet.Stream) {
		handler(&MsgStream{stream, gob.NewEncoder(stream), gob.NewDecoder(stream)})
	})
}

// NewMsgStream 创建消息流
func (h *Host) NewMsgStream(ctx context.Context, peerID string, protocolID string) (*MsgStream, error) {
	pid, err := peer.IDB58Decode(peerID)
	if err != nil {
		return nil, fmt.Errorf("ID formate fail：%s", err)
	}
	stm, err := h.NewStream(ctx, pid, protocol.ID(protocolID))
	if err != nil {
		return nil, fmt.Errorf("new stream error:%s", err)
	}
	return &MsgStream{stm, gob.NewEncoder(stm), gob.NewDecoder(stm)}, nil
}

// SendMsg 发送消息
func (h *Host) SendMsg(id string, pid string, data []byte) ([]byte, error) {
	stm, err := h.NewMsgStream(context.Background(), id, pid)
	if err != nil {
		return nil, fmt.Errorf("Create stream fail: %s", err)
	}
	return stm.SendMsgGetResponse(data)
}

// InfoFromAddrString 地址转peerInfo
func InfoFromAddrString(addr string) (*peerstore.PeerInfo, error) {
	ma, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return nil, err
	}

	return peerstore.InfoFromP2pAddr(ma)
}
