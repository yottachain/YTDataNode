package node

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/yottachain/YTDataNode/Perf"
	"github.com/yottachain/YTDataNode/TaskPool"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/slicecompare/confirmSlice"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTDataNode/util"
	"log"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/multiformats/go-multiaddr"
	rc "github.com/yottachain/YTDataNode/recover"
	"github.com/yottachain/YTDataNode/remoteDebug"

	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/service"
	ytfs "github.com/yottachain/YTFS"
	yhservice "github.com/yottachain/YTHost/service"
)

type ytfsDisk *ytfs.YTFS

var rms *service.RelayManager
var lt = (&statistics.LastUpTime{}).Read()

func (sn *storageNode) Service() {
	Perf.Sn = sn

	//go config.Gconfig.UpdateService(context.Background(), time.Minute)

	// 初始化统计
	statistics.InitDefaultStat()

	rms = service.NewRelayManage(sn.Host())

	//gc := config.NewGConfig(sn.config)
	//if err := gc.Get(); err != nil {
	//	log.Printf("[gconfig] update error:%s\n", err.Error())
	//}
	//go gc.UpdateService(context.Background(), time.Minute)

	config.Gconfig.OnUpdate = func(gc config.Gcfg) {
		log.Printf("[gconfig]配置更新重启矿机 %v\n", gc)
		config.Gconfig.Save()
		// 随机等待重启，错开高峰
		switch config.Gconfig.Clean {
		case 1:
			os.Remove(path.Join(util.GetYTFSPath(), ".utp_params.json"))
		case 2:
			os.Remove(path.Join(util.GetYTFSPath(), ".dtp_params.json"))
		default:

		}
		time.Sleep(time.Duration(rand.Int63n(10)) * time.Second)
		os.Exit(0)
	}
	var utp *TaskPool.TaskPool = TaskPool.Utp()
	var dtp *TaskPool.TaskPool = TaskPool.Dtp()
	// 统计归零
	utp.OnChange(func(pt *TaskPool.TaskPool) {
		atomic.StoreInt64(&statistics.DefaultStat.SaveRequestCount, 0)
		atomic.StoreInt64(&statistics.DefaultStat.RequestToken, 0)
	})
	dtp.OnChange(func(pt *TaskPool.TaskPool) {
		atomic.StoreInt64(&statistics.DefaultStat.RequestDownloadToken, 0)
	})

	statistics.DefaultStat.TokenQueueLen = 200
	var wh *WriteHandler
	//// 每次更新重置utp
	//gc.OnUpdate = func(c config.Gcfg) {
	//	//utp = uploadTaskPool.New(gc.MaxConn, gc.TTL, time.Millisecond*gc.TokenInterval)
	//	log.Println("[gconfig]", "update", gc.MaxConn, gc.TokenInterval, gc.TTL)
	//	//wh = NewWriteHandler(sn, utp)
	//	//wh.Run()
	//	os.Exit(0)
	//}
	//fmt.Printf("[task pool]pool number %d\n", maxConn)

	wh = NewWriteHandler(sn)

	wh.Run()
	_ = sn.Host().RegisterHandler(message.MsgIDNodeCapacityRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		res := wh.GetToken(data, head.RemotePeerID, head.RemoteAddrs)
		if res == nil || len(res) < 3 {
			return nil, fmt.Errorf("no token")
		}
		return res, nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDUploadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		statistics.AddCounnectCount(head.RemotePeerID)
		defer statistics.SubCounnectCount(head.RemotePeerID)
		return wh.Handle(data, head), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDDownloadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		dh := DownloadHandler{sn}
		log.Printf("[download] get shard request from %s\n request buf %s\n", head.RemotePeerID.Pretty(), hex.EncodeToString(data))
		return dh.Handle(data, head.RemotePeerID)
	})
	// 下载回执
	_ = sn.Host().RegisterHandler(message.MsgIDDownloadTKCheck.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		var msg message.DownloadTKCheck
		if err := proto.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		var tk TaskPool.Token
		tk.FillFromString(msg.Tk)
		TaskPool.Dtp().NetLatency.Add(time.Now().Sub(tk.Tm))
		TaskPool.Dtp().Delete(&tk)
		return nil, nil
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

	rce.EncodeForRecover()
	//go rce.Run()
	go rce.RunPool()

	_ = sn.Host().RegisterHandler(message.MsgIDMultiTaskDescription.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		if err := rce.HandleMuilteTaskMsg(data); err == nil {
			log.Println("[recover]success")
		} else {
			log.Println("[recover]error", err)
		}

		//记录上次数据
		//go func() {
		//	fd, _ := os.OpenFile(path.Join(util.GetYTFSPath(), fmt.Sprintf("rcpackage.data")), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		//	defer fd.Close()
		//	fd.Write(data)
		//}()
		return message.MsgIDVoidResponse.Bytes(), nil
	})

	_ = sn.Host().RegisterHandler(message.MsgIDDownloadYTFSFile.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		err := remoteDebug.Handle(data)
		if err != nil {
			log.Println("[debug]", err)
		}
		return message.MsgIDVoidResponse.Bytes(), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDDebug.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		err := remoteDebug.Handle2(data)
		if err != nil {
			log.Println("[debug]", err)
		}
		return message.MsgIDVoidResponse.Bytes(), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDSelfVerifyReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		vfs := verifySlice.VerifySler{sn}
		resp := vfs.VerifySlice()
		return append(message.MsgIDSelfVerifyResp.Bytes(), resp...), nil
	})

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
		tk := TaskPool.NewToken()
		tk.FillFromString(msg.AllocId)
		TaskPool.Utp().Delete(tk)
		log.Println("test upload return", head.RemotePeerID)
		return append(message.MsgIDUploadShard2CResponse.Bytes(), buf...), err
	})

	// 测试矿机性能
	_ = sn.Host().RegisterHandler(message.MsgIDTestMinerPerfTask.Value(), func(requestData []byte, head yhservice.Head) (bytes []byte, err error) {
		return Perf.TestMinerPerfHandler(requestData)
	})
	_ = sn.Host().RegisterHandler(message.MsgIDTestGetBlock.Value(), func(requestData []byte, head yhservice.Head) (bytes []byte, err error) {
		return Perf.GetBlock(requestData)
	})

	go sn.Host().Accept()
	//Register(sn)
	go func() {
		for {
			Report(sn, rce)
			time.Sleep(time.Second * 60)
		}
	}()
}

var first = true

// Report 上报状态
func Report(sn *storageNode, rce *rc.RecoverEngine) {
	var msg message.StatusRepReq
	if len(sn.Config().BPList) == 0 {
		log.Println("no bp")
		return
	}
	bp := sn.Config().BPList[sn.GetBP()]
	//log.Println("bplist:",sn.Config().BPList,"bpindex:",sn.GetBP(),bp)
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

	statistics.DefaultStat.Lock()
	statistics.DefaultStat.AvailableTokenNumber = TaskPool.Utp().FreeTokenLen()
	statistics.DefaultStat.UseKvDb = sn.config.UseKvDb
	statistics.DefaultStat.TokenFillSpeed = TaskPool.Utp().GetTFillTKSpeed()
	if int(statistics.DefaultStat.TokenFillSpeed) > config.Gconfig.MaxToken {
		statistics.DefaultStat.TokenFillSpeed = 100
	}
	statistics.DefaultStat.DownloadTokenFillSpeed = TaskPool.Dtp().GetTFillTKSpeed()
	statistics.DefaultStat.SentToken, statistics.DefaultStat.SaveSuccessCount = TaskPool.Utp().GetParams()
	statistics.DefaultStat.SentDownloadToken, statistics.DefaultStat.DownloadSuccessCount = TaskPool.Dtp().GetParams()
	statistics.DefaultStat.Connection = statistics.GetConnectionNumber()
	statistics.DefaultStat.NetLatency = TaskPool.Utp().NetLatency.Avg()
	statistics.DefaultStat.DiskLatency = TaskPool.Utp().DiskLatency.Avg()
	statistics.DefaultStat.DownloadNetLatency = TaskPool.Dtp().NetLatency.Avg()
	statistics.DefaultStat.DownloadDiskLatency = TaskPool.Dtp().DiskLatency.Avg()
	statistics.DefaultStat.Unlock()
	statistics.DefaultStat.Mean()
	statistics.DefaultStat.GconfigMd5 = config.Gconfig.MD5()
	statistics.DefaultStat.RebuildShardStat = rce.GetStat()
	statistics.DefaultStat.Ban = false
	if time.Now().Sub(lt) < time.Duration(config.Gconfig.BanTime)*time.Second {
		statistics.DefaultStat.Ban = true
		statistics.DefaultStat.TokenFillSpeed = 1
	}

	TaskPool.Utp().Save()
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
