package host

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	"github.com/libp2p/go-libp2p-core/peer"
	ci "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	protocol "github.com/libp2p/go-libp2p-protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
	"io/ioutil"
)

// Host 节点host
type Host struct {
	host.Host
	superNodes []*peerstore.PeerInfo
	privKey    ci.PrivKey
}

type PubKey ci.PubKey
type PrivKey ci.PrivKey

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
func (h *Host) SetPrivKey(pk ci.PrivKey) error {
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
func (h *Host) Daemon(ctx context.Context, cfg config.Config) error {
	//setupSigusr1Trap()
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddr),
		libp2p.NATPortMap(),
		// libp2p.Transport(quictpt.NewTransport),
		libp2p.DefaultTransports,
		libp2p.EnableRelay(circuit.OptHop, circuit.OptDiscovery),
		//libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		//		//	for i, addr := range addrs {
		//		//		pls := addr.Protocols()
		//		//		pls[1].
		//		//	}
		//		//	return addrs
		//		//}),
	}
	if h.privKey != nil {
		opts = append(opts, libp2p.Identity(h.privKey))
	}
	host, err := libp2p.New(
		ctx,
		opts...,
	)
	if err != nil {
		log.Println("p2p host init fail:", err)
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
	log.Println("send msg success")
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

//func setupSigusr1Trap() {
//	c := make(chan os.Signal, 1)
//	signal.Notify(c, syscall.SIGUSR1)
//	go func() {
//		for range c {
//			DumpStacks()
//		}
//	}()
//}
//func DumpStacks() {
//	buf := make([]byte, 1<<20)
//	buf = buf[:runtime.Stack(buf, true)]
//	fmt.Printf("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
//}
