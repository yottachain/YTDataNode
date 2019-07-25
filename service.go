package node

import (
	"context"
	"github.com/yottachain/YTDataNode/logger"
	"os"

	"github.com/gogo/protobuf/proto"

	"time"

	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/service"

	ytfs "github.com/yottachain/YTFS"
)

type ytfsDisk *ytfs.YTFS

var rms *service.RelayManager

func (sn *storageNode) Service() {
	hm := service.NewHandleMsgService(sn.host)
	hm.RegitsterHandler("/node/0.0.1", message.MsgIDUploadShardRequest.Value(), func(data []byte) []byte {
		wh := WriteHandler{sn}
		return wh.Handle(data)
	})
	hm.RegitsterHandler("/node/0.0.1", message.MsgIDDownloadShardRequest.Value(), func(data []byte) []byte {
		dh := DownloadHandler{sn}
		return dh.Handle(data)
	})
	hm.RegitsterHandler("/node/0.0.1", message.MsgIDString.Value(), func(data []byte) []byte {
		log.Println("ping")
		return append(message.MsgIDString.Bytes(), []byte("pong")...)
	})
	hm.RegitsterHandler("/node/0.0.1", message.MsgIDSpotCheckTaskList.Value(), func(data []byte) []byte {
		sch := SpotCheckHandler{sn}
		return sch.Handle(data)
	})
	hm.Service()
	rms = service.NewRelayManage(sn.host)
	rms.Service()
	//Register(sn)
	go func() {
		for {
			Report(sn)
			time.Sleep(time.Second * 60)
		}
	}()
	// for _, v := range sn.services {
	// 	v.Service()
	// }
	go func() {
		for {
			sn.Config().ReloadBPList()
			log.Println("更新BPLIST")
			time.Sleep(time.Hour)
		}
	}()
}

// Register 注册矿机
func Register(sn *storageNode) {
	var msg message.NodeRegReq
	msg.Nodeid = sn.Host().ID().Pretty()
	msg.Owner = os.Getenv("owner")
	log.Println("owner:", msg.Owner)
	msg.Addrs = sn.Addrs()
	msg.MaxDataSpace = sn.YTFS().Meta().YtfsSize
	msg.Relay = sn.config.Relay

	bp := sn.Config().BPList[sn.GetBP()]
	if err := sn.Host().ConnectAddrStrings(bp.ID, bp.Addrs); err != nil {
		log.Println("Connect bp fail", err)
	}
	msgBytes, err := proto.Marshal(&msg)
	if err != nil {
		log.Println("Formate msg fail:", err)
	}
	log.Println("sn index:", sn.GetBP())
	stm, err := sn.host.NewMsgStream(context.Background(), bp.ID, "/node/0.0.1")
	if err != nil {
		log.Println("Create MsgStream fail:", err)
	} else {
		res, err := stm.SendMsgGetResponse(append(message.MsgIDNodeRegReq.Bytes(), msgBytes...))
		if err != nil {
			log.Println("Send reg msg fail:", err)
		} else {
			var resMsg message.NodeRegResp
			proto.Unmarshal(res[2:], &resMsg)
			sn.Config().IndexID = resMsg.Id
			sn.Config().Save()
			sn.owner.BuySpace = resMsg.AssignedSpace
			log.Printf("id %d, Reg success, distribution space %d\n", resMsg.Id, resMsg.AssignedSpace)
			if resMsg.RelayUrl != "" {
				rms.UpdateAddr(resMsg.RelayUrl)
				log.Printf("update relay addr: %s\n", resMsg.RelayUrl)
			}
		}
	}
}

// Report 上报状态
func Report(sn *storageNode) {
	var msg message.StatusRepReq
	bp := sn.Config().BPList[sn.GetBP()]
	msg.Addrs = sn.Addrs()
	if rms.Addr() != "" {
		msg.Addrs = append(sn.Addrs(), rms.Addr())
	} else {
		msg.Addrs = sn.Addrs()
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
	if err := sn.Host().ConnectAddrStrings(bp.ID, bp.Addrs); err != nil {
		log.Println("Connect bp fail", err)
	}
	stm, err := sn.host.NewMsgStream(context.Background(), bp.ID, "/node/0.0.1")
	if err != nil {
		log.Println("Create MsgStream fail:", err)
	} else {
		res, err := stm.SendMsgGetResponse(append(message.MsgIDStatusRepReq.Bytes(), resData...))
		if err != nil {
			log.Println("Send report msg fail:", err)
		} else {
			var resMsg message.StatusRepResp
			proto.Unmarshal(res[2:], &resMsg)
			sn.owner.BuySpace = resMsg.ProductiveSpace
			log.Printf("report info success: %d, relay:%s\n", resMsg.ProductiveSpace, resMsg.RelayUrl)
			if resMsg.RelayUrl != "" {
				rms.UpdateAddr(resMsg.RelayUrl)
				log.Printf("update relay addr: %s\n", resMsg.RelayUrl)
			} else {
				rms.UpdateAddr("")
			}
		}
	}
}
