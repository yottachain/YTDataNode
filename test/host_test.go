package test

import (
	"context"
	"github.com/yottachain/YTDataNode/logger"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-peer"

	"github.com/libp2p/go-libp2p-peerstore"

	"github.com/yottachain/YTDataNode/host"
)

const (
	m1 = "/ip4/0.0.0.0/tcp/9001"
	m2 = "/ip4/0.0.0.0/tcp/9002"
)

func TestNewHost(t *testing.T) {
	h := host.NewP2PHost()
	if err := h.Daemon(context.Background(), m1); err != nil {
		t.Error(err)
	} else {
		t.Log(h)
	}
	h2 := host.NewP2PHost()
	if err := h2.Daemon(context.Background(), m1); err != nil {
		t.Error(err)
	}
	h.HandleMessage("msg", func(msg *host.MsgStream) {
		if string(msg.Content()) != "1111" {
			t.Error("消息校验失败")
		}
		msg.SendMsgClose([]byte("222"))
	})
	err := h2.Connect(context.Background(), peerstore.PeerInfo{
		h.ID(),
		h.Network().ListenAddresses(),
	})
	if err != nil {
		t.Error(err)
	}
	ms, err := h2.NewMsgStream(context.Background(), h.ID().Pretty(), "msg")
	if err != nil {
		t.Error(err)
	} else {
		res, err := ms.SendMsgGetResponse([]byte("1111"))
		if err != nil {
			t.Error(err)
		} else {
			if string(res) == "222" {
				t.Log("success")
			} else {
				t.Error("fail")
			}
		}
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
	<-ctx.Done()
}

func TestAddPeer(t *testing.T) {
	h := host.NewP2PHost()
	if err := h.Daemon(context.Background(), m1); err != nil {
		t.Error(err)
	} else {
		t.Log(h)
	}
	h.ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{m1})
	h.ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{m1})
	h.ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{m2})
	h.ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{m1})
	err := h.ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{m1})
	if err != nil {
		t.Error(err)
	}
	id, _ := peer.IDB58Decode("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi")
	log.Println(h.Peerstore().Addrs(id))
}
