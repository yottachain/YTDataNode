package randDownload

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/activeNodeList"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/storageNodeInterface"
	"math/rand"
	"time"
)

var Sn storageNodeInterface.StorageNode

func GetRandNode() (*peer.AddrInfo, error) {
	nodeList := activeNodeList.GetNodeList()
	randIndex := rand.Intn(len(nodeList))
	randNode := nodeList[randIndex]

	pi := &peer.AddrInfo{}
	id, err := peer.IDFromString(randNode.NodeID)
	if err != nil {
		return nil, err
	}
	pi.ID = id
	for _, v := range randNode.IP {
		ma, err := multiaddr.NewMultiaddr(v)
		if err != nil {
			continue
		}
		pi.Addrs = append(pi.Addrs, ma)
	}
	return pi, nil
}

func DownloadFromRandNode() error {
	pi, err := GetRandNode()
	if err != nil {
		return nil
	}

	if Sn == nil {
		return fmt.Errorf("no storage-node")
	}

	ctx, cancle := context.WithTimeout(context.Background(), time.Second*30)
	defer cancle()

	clt, err := Sn.Host().ClientStore().Get(ctx, pi.ID, pi.Addrs)
	if err != nil {
		return err
	}

	var getTokenMsg message.NodeCapacityRequest
	getTokenMsg.RequestMsgID = message.MsgIDDownloadShardRequest.Value() + 1
	getTKMsgBuf, err := proto.Marshal(&getTokenMsg)
	if err != nil {
		return err
	}
	getTKResBuf, err := clt.SendMsg(ctx, message.MsgIDNodeCapacityRequest.Value(), getTKMsgBuf)
	if err != nil {
		return nil
	}

	return nil
}
