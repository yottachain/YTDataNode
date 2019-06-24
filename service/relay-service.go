package service

import (
	"fmt"
	"regexp"
	"time"

	peerstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/host"
)

// RelayManager 中继管理
type RelayManager struct {
	host *host.Host
	peer *peerstore.PeerInfo
	addr string
}

// NewRelayManage 创建新的中继管理器
func NewRelayManage(hst *host.Host) *RelayManager {
	rm := new(RelayManager)
	rm.host = hst
	return rm
}

// UpdateAddr 更新中继地址
func (rm *RelayManager) UpdateAddr(addr string) error {
	if addr == "" {
		return fmt.Errorf("addr required")
	}
	rm.addr = addr

	rp, _ := regexp.Compile("/p2p-.+$")
	res := rp.ReplaceAllString(addr, "")
	ma, err := multiaddr.NewMultiaddr(res)
	if err == nil {
		pi, err := peerstore.InfoFromP2pAddr(ma)
		if err != nil {
			return err
		}
		rm.peer = pi
	}
	// fmt.Println(err, "中继地址解析错误")
	return err
}

// Addr 返回addr
func (rm *RelayManager) Addr() string {
	return rm.addr
}

// Service 启动服务
func (rm *RelayManager) Service() {
	go func() {
		for {
			rm.ping()
			<-time.After(time.Second * 30)
		}
	}()
}

func (rm *RelayManager) ping() error {
	//rm.host.Connect(context.Background(), *rm.peer)
	//stm, _ := rm.host.NewMsgStream(context.Background(), rm.peer.ID.Pretty(), "/node/0.0.0.1")
	//stm.SendMsg(append(message.MsgIDString.Bytes(), []byte("ping")...))
	return nil
}
