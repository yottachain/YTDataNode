package node

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/logger"
	rc "github.com/yottachain/YTDataNode/recover"
	"github.com/yottachain/YTDataNode/uploadTaskPool"
	"time"

	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/service"

	yhservice "github.com/graydream/YTHost/service"
	ytfs "github.com/yottachain/YTFS"
)

type ytfsDisk *ytfs.YTFS

var rms *service.RelayManager

func (sn *storageNode) Service() {

	rms = service.NewRelayManage(sn.Host())

	maxConn := sn.Config().MaxConn
	if maxConn == 0 {
		maxConn = 100
	}

	tokenInterval := sn.Config().TokenInterval
	if tokenInterval == 0 {
		tokenInterval = 50
	}

	fmt.Printf("[task pool]pool number %d\n", maxConn)
	wh := NewWriteHandler(sn, uploadTaskPool.New(maxConn, time.Second*10, tokenInterval*time.Millisecond))
	wh.Run()
	_ = sn.Host().RegisterHandler(message.MsgIDNodeCapacityRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		return wh.GetToken(data), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDUploadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		return wh.Handle(data), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDDownloadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		dh := DownloadHandler{sn}
		return dh.Handle(data), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDString.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		return append(message.MsgIDString.Bytes(), []byte("pong")...), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDSpotCheckTaskList.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		sch := SpotCheckHandler{sn}
		return sch.Handle(data), nil
	})
	rce, err := rc.New(sn)
	if err != nil {
		log.Printf("[recover]init error %s\n", err.Error())
	}

	go rce.Run()

	//sn.Host().RegisterHandler(message.MsgIDMultiTaskDescription.Value(), func(data []byte, rp peerInfo.PeerInfo) ([]byte, error) {
	//	if err := rce.HandleMuilteTaskMsg(data, rp); err == nil {
	//		log.Println("[recover]success")
	//	} else {
	//		log.Println("[recover]error", err)
	//	}
	//
	//	// 记录上次数据
	//	go func() {
	//		fd, _ := os.OpenFile(path.Join(util.GetYTFSPath(), fmt.Sprintf("%d-test.data", time.Now().UnixNano())), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	//		defer fd.Close()
	//		fd.Write(data)
	//	}()
	//	return message.MsgIDVoidResponse.Bytes(), nil
	//})
	go sn.Host().Accept()
	//Register(sn)
	go func() {
		for {
			Report(sn)
			time.Sleep(time.Second * 60)
		}
	}()
}

var first = true

// Report 上报状态
func Report(sn *storageNode) {
	var msg message.StatusRepReq
	if len(sn.Config().BPList) == 0 {
		log.Println("no bp")
		return
	}
	bp := sn.Config().BPList[sn.GetBP()]
	msg.Addrs = sn.Addrs()
	if rms.Addr() != "" && first == false {
		msg.Addrs = append(sn.Addrs(), rms.Addr())
	} else {
		msg.Addrs = sn.Addrs()
		if first == true {
			first = false
		}
	}

	msg.Cpu = sn.Runtime().AvCPU
	msg.Memory = sn.Runtime().Mem
	msg.Id = sn.Config().IndexID
	msg.MaxDataSpace = sn.YTFS().Meta().YtfsSize / uint64(sn.YTFS().Meta().DataBlockSize)
	msg.UsedSpace = sn.YTFS().Len() / uint64(sn.YTFS().Meta().DataBlockSize)

	msg.Relay = sn.config.Relay
	msg.Version = sn.config.Version()
	resData, err := proto.Marshal(&msg)
	log.Printf("cpu:%d%% mem:%d%% max-space: %d block\n", msg.Cpu, msg.Memory, msg.MaxDataSpace)
	if err != nil {
		log.Println("send report msg fail:", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if clt, err := sn.Host().ConnectAddrStrings(ctx, bp.ID, bp.Addrs); err != nil {
		log.Println("Connect bp fail", err)
	} else {
		log.Printf("Report to %s:%v\n", bp.ID, bp.Addrs)
		res, err := clt.SendMsg(ctx, message.MsgIDStatusRepReq.Value(), resData)
		defer clt.Close()
		if err != nil {
			log.Println("Send report msg fail:", err)
		} else {
			var resMsg message.StatusRepResp
			proto.Unmarshal(res[2:], &resMsg)
			sn.owner.BuySpace = resMsg.ProductiveSpace
			log.Printf("report info success: %d, relay:%s\n", resMsg.ProductiveSpace, resMsg.RelayUrl)
			if resMsg.RelayUrl != "" {
				if _, err := multiaddr.NewMultiaddr(resMsg.RelayUrl); err != nil {
					return
				}
				rms.UpdateAddr(resMsg.RelayUrl)
				log.Printf("update relay addr: %s\n", resMsg.RelayUrl)
			} else {
				rms.UpdateAddr("")
			}
		}
	}
}
