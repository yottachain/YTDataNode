package node

import (
	"bufio"
	"context"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/statistics"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/multiformats/go-multiaddr"
	rc "github.com/yottachain/YTDataNode/recover"
	"github.com/yottachain/YTDataNode/remoteDebug"
	"github.com/yottachain/YTDataNode/uploadTaskPool"

	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/service"

	ytfs "github.com/yottachain/YTFS"
	yhservice "github.com/yottachain/YTHost/service"
)

type ytfsDisk *ytfs.YTFS

var rms *service.RelayManager

func (sn *storageNode) Service() {

	// 初始化统计
	statistics.InitDefaultStat()

	rms = service.NewRelayManage(sn.Host())

	gc := config.NewGConfig(sn.config)
	if err := gc.Get(); err != nil {
		log.Printf("[gconfig] update error:%s\n", err.Error())
	}
	go gc.UpdateService(context.Background(), time.Minute)

	var utp *uploadTaskPool.UploadTaskPool = uploadTaskPool.New(gc.MaxConn, gc.TTL, time.Millisecond*gc.TokenInterval)
	statistics.DefaultStat.TokenQueueLen = gc.MaxConn
	var wh *WriteHandler
	// 每次更新重置utp
	gc.OnUpdate = func(c config.Gcfg) {
		//utp = uploadTaskPool.New(gc.MaxConn, gc.TTL, time.Millisecond*gc.TokenInterval)
		log.Println("[gconfig]", "update", gc.MaxConn, gc.TokenInterval, gc.TTL)
		//wh = NewWriteHandler(sn, utp)
		//wh.Run()
		os.Exit(0)
	}

	//fmt.Printf("[task pool]pool number %d\n", maxConn)
	wh = NewWriteHandler(sn, utp)
	wh.Run()
	_ = sn.Host().RegisterHandler(message.MsgIDNodeCapacityRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		return wh.GetToken(data, head.RemotePeerID), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDUploadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		return wh.Handle(data), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDDownloadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		dh := DownloadHandler{sn}
		return dh.Handle(data, head.RemotePeerID), nil
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

	// _ = sn.Host().RegisterHandler(message.MsgIDMultiTaskDescription.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
	// 	if err := rce.HandleMuilteTaskMsg(data); err == nil {
	// 		log.Println("[recover]success")
	// 	} else {
	// 		log.Println("[recover]error", err)
	// 	}

	// 	// 记录上次数据
	// 	//go func() {
	// 	//	fd, _ := os.OpenFile(path.Join(util.GetYTFSPath(), fmt.Sprintf("rcpackage.data")), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	// 	//	defer fd.Close()
	// 	//	fd.Write(data)
	// 	//}()
	// 	return message.MsgIDVoidResponse.Bytes(), nil
	// })

	_ = sn.Host().RegisterHandler(message.MsgIDDownloadYTFSFile.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		err := remoteDebug.Handle(data)
		if err != nil {
			log.Println("[debug]", err)
		}
		return message.MsgIDVoidResponse.Bytes(), err
	})
	//_ = sn.Host().RegisterHandler(message.MsgIDMultiTaskDescription.Value(), func(requestData []byte, head yhservice.Head) ([]byte, error) {
	//	go func(data []byte) {
	//		var msg message.MultiTaskDescription
	//		if err := proto.Unmarshal(data, &msg); err != nil {
	//			log.Println("[recover error]", err)
	//		}
	//		for _, v := range msg.Tasklist {
	//			if bytes.Equal(message.MsgIDLRCTaskDescription.Bytes(), v[0:2]) {
	//				var tmsg message.TaskDescription
	//				err := proto.Unmarshal(v[2:], &tmsg)
	//				if err != nil {
	//					log.Println("[recover error]", err)
	//					continue
	//				}
	//
	//				var res message.TaskOpResult
	//				res.RES = 0
	//				res.Id = tmsg.Id
	//
	//				bpid := tmsg.Id[12]
	//
	//				resData, err := proto.Marshal(&res)
	//				if err != nil {
	//					log.Println("[recover error]", err)
	//					continue
	//				}
	//
	//				_, err = sn.SendBPMsg(int(bpid), message.MsgIDTaskOPResult.Value(), resData)
	//				if err != nil {
	//					log.Println("[recover error]", err)
	//					continue
	//				}
	//			}
	//		}
	//	}(requestData)
	//	return message.MsgIDVoidResponse.Bytes(), nil
	//})
	_ = sn.Host().RegisterHandler(message.MsgIDSleepReturn.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		var msg message.UploadShardRequestTest
		if err := proto.Unmarshal(data, &msg); err == nil {
			log.Println("[sleep]", msg.Sleep)
			<-time.After(time.Duration(msg.Sleep) * time.Millisecond)
		} else {
			log.Println("[sleep]", err)
		}
		var res message.UploadShard2CResponse
		res.RES = 0

		buf, err := proto.Marshal(&res)

		return append(message.MsgIDUploadShard2CResponse.Bytes(), buf...), err
	})
	go sn.Host().Accept()
	//Register(sn)
	go func() {
		for {
			Report(sn, rce, utp)
			time.Sleep(time.Second * 60)
		}
	}()
}

var first = true

// Report 上报状态
func Report(sn *storageNode, rce *rc.RecoverEngine, pool *uploadTaskPool.UploadTaskPool) {
	var msg message.StatusRepReq
	if len(sn.Config().BPList) == 0 {
		log.Println("no bp")
		return
	}
	bp := sn.Config().BPList[sn.GetBP()]
	log.Println("bplist:",sn.Config().BPList,"bpindex:",bp)
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
	msg.UsedSpace = sn.YTFS().Len()
	msg.RealSpace = uint32(sn.YTFS().Len())

	msg.Relay = sn.config.Relay
	msg.Version = sn.config.Version()
	msg.Rx = GetXX("R")
	msg.Tx = GetXX("T")

	statistics.DefaultStat.AvailableTokenNumber = pool.FreeTokenLen()
	msg.Other = fmt.Sprintf("[%s]", statistics.DefaultStat.String())
	log.Println("[report] other:", msg.Other)

	if rce.Len() == 0 {
		msg.Rebuilding = 0
	} else {
		msg.Rebuilding = 1
	}

	resData, err := proto.Marshal(&msg)
	log.Printf("RX:%d,TX:%d\n", msg.Rx, msg.Tx)
	log.Printf("cpu:%d%% mem:%d%% max-space: %d block\n", msg.Cpu, msg.Memory, msg.MaxDataSpace)
	if err != nil {
		log.Println("send report msg fail:", err)
	}
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	//defer cancel()

	//if clt, err := sn.Host().ClientStore().GetByAddrString(ctx, bp.ID, bp.Addrs); err != nil {
	//	log.Println("Connect bp fail", err)
	//} else {
	//
	//}

	log.Printf("Report to %s:%v\n", bp.ID, bp.Addrs)
	//res, err := clt.SendMsg(ctx, message.MsgIDStatusRepReq.Value(), resData)
	res, err := sn.SendBPMsg(sn.GetBP(), message.MsgIDStatusRepReq.Value(), resData)
	//defer clt.Close()
	if err != nil {
		log.Println("Send report msg fail:", err)
	} else {
		var resMsg message.StatusRepResp
		proto.Unmarshal(res[2:], &resMsg)
		sn.owner.BuySpace = resMsg.ProductiveSpace
		log.Printf("report info success: %d, relay:%s\n", resMsg.ProductiveSpace, resMsg.RelayUrl)
		if resMsg.RelayUrl != "" {
			if _, err := multiaddr.NewMultiaddr(resMsg.RelayUrl); err == nil {
				rms.UpdateAddr(resMsg.RelayUrl)
				log.Printf("update relay addr: %s\n", resMsg.RelayUrl)
			} else {
				rms.UpdateAddr("")
			}
		} else {
			rms.UpdateAddr("")
		}
	}
}

func GetXX(rt string) uint64 {

	nf, err := os.OpenFile("/proc/net/dev", os.O_RDONLY, 0644)
	if err != nil {
		return 0
	}
	sc := bufio.NewScanner(nf)

	var rx, tx uint64

	for sc.Scan() {
		line := sc.Text()
		arr := strings.Split(line, ":")
		if len(arr) > 1 {
			reg := regexp.MustCompile(" +")
			arr2 := reg.Split(arr[1], -1)
			r, _ := strconv.ParseUint(arr2[1], 10, 64)
			t, _ := strconv.ParseUint(arr2[9], 10, 64)
			rx = rx + r
			tx = tx + t
		}
	}

	switch rt {
	case "r", "R":
		return uint64(rx)
	case "t", "T":
		return uint64(tx)
	default:
		return 0
	}
}
