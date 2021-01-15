package randDownload

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/TaskPool"
	"github.com/yottachain/YTDataNode/activeNodeList"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTDataNode/storageNodeInterface"
	"math/rand"
	"sync/atomic"
	"time"
)

var errNoTK = fmt.Errorf("notk")

var Sn storageNodeInterface.StorageNode

func GetRandNode() (*peer.AddrInfo, error) {
	nodeList := activeNodeList.GetNodeListByTimeAndGroupSize(time.Minute*time.Duration(config.Gconfig.NodeListUpdateTime), config.Gconfig.RandDownloadGroupSize)

	pi := &peer.AddrInfo{}
	nl := len(nodeList)

	var randNode activeNodeList.Data

	if nl <= 0 {
		return nil, fmt.Errorf("no node")
	}
	randIndex := rand.Intn(nl)
	randNode = nodeList[randIndex]

	id, err := peer.IDB58Decode(randNode.NodeID)
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

func DownloadFromRandNode(utk *TaskPool.Token, ctx context.Context) error {
	var err error

	pi, err := GetRandNode()
	if err != nil {
		return err
	}
	//log.Println("[randDownload] test node", pi.ID, pi.Addrs)

	if Sn == nil {
		return fmt.Errorf("no storage-node")
	}

	clt, err := Sn.Host().ClientStore().Get(ctx, pi.ID, pi.Addrs)
	if err != nil {
		return err
	}
	defer clt.Close()

	var getTokenMsg message.NodeCapacityRequest
	getTokenMsg.RequestMsgID = message.MsgIDTestGetBlock.Value()
	getTKMsgBuf, err := proto.Marshal(&getTokenMsg)
	if err != nil {
		return errNoTK
	}

	getTKResBuf, err := clt.SendMsg(ctx, message.MsgIDNodeCapacityRequest.Value(), getTKMsgBuf)
	if err != nil {
		return errNoTK
	}
	var tokenMsg message.NodeCapacityResponse
	err = proto.Unmarshal(getTKResBuf[2:], &tokenMsg)
	if err != nil {
		return errNoTK
	}

	var downloadMsg message.TestGetBlock
	downloadMsg.Msg = make([]byte, 16*1024)
	rand.Read(downloadMsg.Msg)
	downloadBuf, err := proto.Marshal(&downloadMsg)
	if err != nil {
		return err
	}
	statistics.DefaultStat.RXTest.AddCount()
	_, err = clt.SendMsg(ctx, message.MsgIDTestGetBlock.Value(), downloadBuf)
	if err != nil {
		return err
	}

	//var checkTKMsg message.DownloadTKCheck
	//checkTKMsg.Tk = "getBlockTK"
	//checkTKBuf, err := proto.Marshal(&checkTKMsg)
	//if err != nil {
	//	return err
	//}
	//
	//_, err = clt.SendMsg(ctx, message.MsgIDDownloadTKCheck.Value(), checkTKBuf)
	//if err != nil {
	//	return err
	//}
	statistics.DefaultStat.RXTest.AddSuccess()
	return nil
}

func Run() {
	var successCount uint64
	var errorCount uint64
	var execCount int64

	go func() {
		for {
			<-time.After(time.Minute)
			log.Println("[randDownload] success", successCount, "error", errorCount, "exec", atomic.LoadInt64(&execCount))
		}
	}()
	for {
		ec := atomic.LoadInt64(&execCount)
		if (ec < int64(TaskPool.Utp().GetTFillTKSpeed())/2 && ec < int64(config.Gconfig.RandDownloadNum)) || ec <= 2 {
			go func() {
				ctx, cancle := context.WithTimeout(context.Background(), time.Second*30)
				defer cancle()
				utk, err := TaskPool.Utp().Get(ctx, Sn.Host().Config().ID, 0)
				if err != nil {
					return
				}
				defer TaskPool.Utp().Delete(utk)

				atomic.AddInt64(&execCount, 1)
				defer atomic.AddInt64(&execCount, -1)

				err = DownloadFromRandNode(utk, ctx)
				if err != nil && err.Error() != errNoTK.Error() {
					//log.Println(err.Error(), errNoTK.Error())
					atomic.AddUint64(&errorCount, 1)
				} else if err == nil {
					atomic.AddUint64(&successCount, 1)
				}
			}()
		}

		<-time.After(time.Millisecond * 10)
	}
}
