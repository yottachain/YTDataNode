package service

import (
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/yottachain/YTDataNode/logger"
	"regexp"
	"time"

	"github.com/multiformats/go-multiaddr"
	. "github.com/yottachain/YTHost/hostInterface"
)

// RelayManager 中继管理
type RelayManager struct {
	host Host
	peer *peer.AddrInfo
	addr string
}

// NewRelayManage 创建新的中继管理器
func NewRelayManage(hst Host) *RelayManager {
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
		pi, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return err
		}
		rm.peer = pi
	}
	// log.Println(err, "中继地址解析错误")
	return err
}

func (rm *RelayManager) ClearRelayAddrs() {
	rm.peer = nil
	rm.addr = ""
}

// Addr 返回addr
func (rm *RelayManager) Addr() string {
	return rm.addr
}

// Service 启动服务
func (rm *RelayManager) Service() {
	go func() {
		for {
			defer func() {
				err := recover()
				if err != nil {
					log.Println("Error:", err)
				}
			}()
			rm.ping()
			<-time.After(time.Second * 60)
		}
	}()
}

func (rm *RelayManager) ping() error {
	// 中继维持存在bug先关闭中继维持。定期更换中继
	//if rm.peer == nil {
	//	return fmt.Errorf("relay peer is nil")
	//}
	//ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	//rm.host.Connect(ctx, *rm.peer)
	//stm, _ := rm.host.NewMsgStream(context.Background(), rm.peer.ID.Pretty(), "/node/0.0.0.1")
	//stm.SendMsg(append(message.MsgIDString.Bytes(), []byte("ping")...))
	//stm.Close()
	//err := recover()
	//if err != nil {
	//	log.Println("中继连接错误", err)
	//	rm.ClearRelayAddrs()
	//}
	rm.ClearRelayAddrs()
	return nil
}
