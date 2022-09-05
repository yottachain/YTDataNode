package node

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/yottachain/YTDataNode/Perf"
	"github.com/yottachain/YTDataNode/TokenPool"
	"github.com/yottachain/YTDataNode/capProof"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/diskHash"
	"github.com/yottachain/YTDataNode/magrate"
	"github.com/yottachain/YTDataNode/randDownload"
	"github.com/yottachain/YTDataNode/setRLimit"
	"github.com/yottachain/YTDataNode/slicecompare"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTDataNode/verifySlice"

	"log"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/multiformats/go-multiaddr"
	rc "github.com/yottachain/YTDataNode/recover"
	"github.com/yottachain/YTDataNode/remoteDebug"

	"github.com/yottachain/YTDataNode/activeNodeList"
	"github.com/yottachain/YTDataNode/gc"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/service"
	yhservice "github.com/yottachain/YTHost/service"
)

var rms *service.RelayManager
var lt = (&statistics.LastUpTime{}).Read()
var disableReport = false
var stopUp = false

func (sn *storageNode) Service() {
	setRLimit.SetRLimit()
	Perf.Sn = sn
	randDownload.Sn = sn

	go config.Gconfig.UpdateService(context.Background(), time.Minute)
	go activeNodeList.UpdateTimer()
	go randDownload.Run()
	go capProof.TimerRun(sn.ytfs)

	//消息注册前 启动gc clean and magrate data
	go func() {
		stopUp = true
		(&gc.GcWorker{sn}).CleanGc()
		var err error
		if sn.config.UseKvDb {
			err = magrate.NewMr().RunRocksdb(sn.ytfs, sn.config.IndexID)
		} else {
			err = magrate.NewMr().RunIndexdb(sn.ytfs, sn.config.IndexID)
		}
		if err != nil {
			log.Printf("%s\n", err.Error())
		}
		stopUp = false
	}()

	// 初始化统计
	statistics.InitDefaultStat()
	// 初始化token池
	InitTokenPool(&statistics.DefaultStat)

	rms = service.NewRelayManage(sn.Host())

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
	var utp = TokenPool.Utp()
	var dtp = TokenPool.Dtp()
	// 统计归零
	utp.OnChange(func(pt *TokenPool.TokenPool) {
		//atomic.StoreInt64(&statistics.DefaultStat.RXRequest, 0)
		//atomic.StoreInt64(&statistics.DefaultStat.RXRequestToken, 0)
	})
	dtp.OnChange(func(pt *TokenPool.TokenPool) {
		//atomic.StoreInt64(&statistics.DefaultStat.TXRequestToken, 0)
	})

	statistics.DefaultStat.TokenQueueLen = 200
	var wh *WriteHandler

	wh = NewWriteHandler(sn)
	if wh == nil {
		log.Println("[error] WriteHandler is nil")
		return
	}

	wh.Run()
	_ = sn.Host().RegisterHandler(message.MsgIDNodeCapacityRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		res := wh.GetToken(data, head.RemotePeerID, head.RemoteAddrs)
		if res == nil || len(res) < 3 || disableReport || stopUp {
			return nil, fmt.Errorf("no token")
		}
		return res, nil
	})

	// 如果进程没有被禁止写入注册上传处理器
	if sn.Config().DisableWrite == false {
		_ = sn.Host().RegisterHandler(message.MsgIDUploadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
			if stopUp {
				log.Println("miner stop upload")
				return nil, fmt.Errorf("miner stop upload")
			}
			log.Println("miner start upload")
			statistics.AddCounnectCount(head.RemotePeerID)
			defer statistics.SubCounnectCount(head.RemotePeerID)
			return wh.Handle(data, head), nil
		})
	}

	slc := &slicecompare.SliceComparer{Sn: sn, Lock: sync.Mutex{}}
	_ = sn.Host().RegisterHandler(message.MsgIDSliceCompareReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		log.Println("[slicecompare] receive compare request!")
		res, _ := slc.CompareMsgChkHdl(data)
		resp, err := proto.Marshal(&res)
		return append(message.MsgIDSliceCompareResp.Bytes(), resp...), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDSliceCompareStatusReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		log.Println("[slicecompare] receive get compare status request!")
		res, err := slc.CompareMsgStatusChkHdl(data)
		resp, err := proto.Marshal(&res)
		return append(message.MsgIDSliceCompareStatusResp.Bytes(), resp...), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDCpDelStatusfileReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		log.Println("[slicecompare] receive delete compare_status request!")
		res, _ := slc.CompareMsgDelfileHdl(data)
		resp, err := proto.Marshal(&res)
		return append(message.MsgIDCpDelStatusfileResp.Bytes(), resp...), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDDownloadShardRequest.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		dh := DownloadHandler{sn}
		log.Printf("[download] get shard request from %s  adds %v\n request buf %s\n",
			head.RemotePeerID.Pretty(), head.RemoteAddrs, hex.EncodeToString(data))
		return dh.Handle(data, head.RemotePeerID)
	})
	// 下载回执
	_ = sn.Host().RegisterHandler(message.MsgIDDownloadTKCheck.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		var msg message.DownloadTKCheck
		if err := proto.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		var tk TokenPool.Token
		tk.FillFromString(msg.Tk)
		lat := time.Now().Sub(tk.Tm)
		if !TokenPool.Dtp().Check(&tk) {
			tmstr := tk.Tm.Format("20060102030405")
			return nil, fmt.Errorf("token time out , token time %s", tmstr)
		}
		TokenPool.Dtp().NetLatency.Add(lat)
		TokenPool.Dtp().Delete(&tk)
		return nil, nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDString.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		return append(message.MsgIDString.Bytes(), []byte("pong")...), nil
	})
	_ = sn.Host().RegisterHandler(message.MsgIDSpotCheckTaskList.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		sch := SpotCheckHandler{sn}
		return sch.Handle(data), nil
	})

	rcv, err := rc.New(sn)
	if err != nil {
		log.Printf("[recover]init error %s\n", err.Error())
		return
	}

	go rcv.RunPool()

	_ = sn.Host().RegisterHandler(message.MsgIDMultiTaskDescription.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		err := rcv.HandleMuilteTaskMsg(data)

		//记录上次数据
		//go func() {
		//	fd, _ := os.OpenFile(path.Join(util.GetYTFSPath(), fmt.Sprintf("rcpackage.data")), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		//	defer fd.Close()
		//	fd.Write(data)
		//}()
		return message.MsgIDVoidResponse.Bytes(), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDGcReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		var resp []byte
		GcW := gc.GcWorker{sn}
		res, err := GcW.GcMsgChkHdl(data)
		resp, _ = proto.Marshal(&res)

		return append(message.MsgIDGcResp.Bytes(), resp...), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDGcStatusReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		GcW := gc.GcWorker{sn}
		res, _ := GcW.GetGcStatusHdl(data)
		resp, err := proto.Marshal(&res)
		return append(message.MsgIDGcStatusResp.Bytes(), resp...), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDGcdelStatusfileReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		GcW := gc.GcWorker{sn}
		res, _ := GcW.GcDelStatusFileHdl(data)
		resp, err := proto.Marshal(&res)
		return append(message.MsgIDGcdelStatusfileResp.Bytes(), resp...), err
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

	vfs := verifySlice.NewVerifySler(sn)
	_ = sn.Host().RegisterHandler(message.MsgIDSelfVerifyReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		var msg message.SelfVerifyReq
		var res message.SelfVerifyResp
		if err := proto.Unmarshal(data, &msg); err != nil {
			log.Println("[verify] message.SelfVerifyReq error:", err)
			res.ErrCode = "100"
			resp, err := proto.Marshal(&res)
			return append(message.MsgIDSelfVerifyResp.Bytes(), resp...), err
		}

		verifynum, _ := strconv.ParseUint(msg.Num, 10, 32)
		startItem := msg.StartItem
		result := vfs.VerifySlice(uint32(verifynum), startItem)
		resp, err := proto.Marshal(result)
		if err != nil {
			log.Println("[verify] Marshal resp error:", err)
		}
		return append(message.MsgIDSelfVerifyResp.Bytes(), resp...), err
	})

	_ = sn.Host().RegisterHandler(message.MsgIDSelfVerifyQueryReq.Value(), func(data []byte, head yhservice.Head) ([]byte, error) {
		var msg message.SelfVerifyQueryReq
		var res message.SelfVerifyQueryResp
		if err := proto.Unmarshal(data, &msg); err != nil {
			log.Println("[verify] message.SelfVerifyReq error:", err)
			res.ErrCode = "ErrUnmarshal"
			//res.ErrCode = "100"
			resp, err := proto.Marshal(&res)
			return append(message.MsgIDSelfVerifyQueryResp.Bytes(), resp...), err
		}

		result := vfs.MissSliceQuery(msg.Key)
		resp, err := proto.Marshal(&result)
		if err != nil {
			log.Println("[debug]", err)
		}
		return append(message.MsgIDSelfVerifyQueryResp.Bytes(), resp...), err
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
		tk := TokenPool.NewToken()
		tk.FillFromString(msg.AllocId)
		atomic.AddInt64(&statistics.DefaultStat.TXSuccess, 1)
		TokenPool.Utp().Delete(tk)
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
			time.Sleep(time.Second * 60)
			Report(sn, rcv)
		}
	}()
}

var first = true

// Report 上报状态
func Report(sn *storageNode, rce *rc.Engine) {
	if disableReport {
		log.Println("miner disable")
		//return
	}

	var msg message.StatusRepReq
	if len(sn.Config().BPList) == 0 {
		log.Println("no bp")
		return
	}

	bp := sn.Config().BPList[sn.GetBP()]
	msg.Addrs, msg.AddrsCmd = sn.Addrs()
	if rms.Addr() != "" && first == false {
		msg.Addrs = append(msg.Addrs, rms.Addr())
		msg.AddrsCmd = append(msg.AddrsCmd, rms.Addr())
	} else {
		msg.Addrs, msg.AddrsCmd = sn.Addrs()
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
	msg.AllocSpace = sn.config.AllocSpace / uint64(sn.YTFS().Meta().DataBlockSize)

	//这个不要实时计算, 后续改成定时计算,然后上报的时候取结果
	msg.AvailableSpace = capProof.GetCapProofSpace(sn.YTFS())
	log.Printf("[cap proof] AvailableSpace %d\n", msg.AvailableSpace)

	msg.Relay = sn.config.Relay
	msg.Version = sn.config.Version()
	msg.Rx = GetXX("R")
	msg.Tx = GetXX("T")

	hash, err := diskHash.GetHash(sn.YTFS())
	if err == nil {
		msg.Hash = hash
	} else {
		log.Println("[diskHash]", err.Error())
	}

	statistics.DefaultStat.Lock()
	statistics.DefaultStat.AvailableTokenNumber = TokenPool.Utp().FreeTokenLen()
	statistics.DefaultStat.UseKvDb = sn.config.UseKvDb
	statistics.DefaultStat.RXTokenFillRate = TokenPool.Utp().GetTFillTKSpeed()
	if int(statistics.DefaultStat.RXTokenFillRate) > config.Gconfig.MaxToken {
		statistics.DefaultStat.RXTokenFillRate = time.Duration(config.Gconfig.MaxToken)
	}
	statistics.DefaultStat.TXTokenFillRate = TokenPool.Dtp().GetTFillTKSpeed()
	//statistics.DefaultStat.SentToken, statistics.DefaultStat.RXSuccess = TokenPool.Utp().GetParams()
	//statistics.DefaultStat.TXToken, statistics.DefaultStat.TXSuccess = TokenPool.Dtp().GetParams()
	statistics.DefaultStat.Connection = sn.Host().ConnStat().GetSerconnCount() + sn.Host().ConnStat().GetCliconnCount()
	//statistics.DefaultStat.Connection = sn.Host().ConnStat().GetSerconnCount()
	statistics.DefaultStat.RXNetLatency = TokenPool.Utp().NetLatency.Avg()
	statistics.DefaultStat.RXDiskLatency = TokenPool.Utp().DiskLatency.Avg()
	statistics.DefaultStat.TXNetLatency = TokenPool.Dtp().NetLatency.Avg()
	statistics.DefaultStat.TXDiskLatency = TokenPool.Dtp().DiskLatency.Avg()
	statistics.DefaultStat.Unlock()
	statistics.DefaultStat.Mean()
	statistics.DefaultStat.GconfigMd5 = config.Gconfig.MD5()
	statistics.DefaultStat.RebuildShardStat = statistics.DefaultRebuildCount.GetStat()
	statistics.DefaultStat.IndexDBOpt = sn.config.Options
	statistics.DefaultStat.Ban = false
	if time.Now().Sub(lt) < time.Duration(config.Gconfig.BanTime)*time.Second {
		statistics.DefaultStat.Ban = true
		statistics.DefaultStat.RXTokenFillRate = 1
	}

	//// 设置重建任务并发限制
	//statistics.DefaultStat.RebuildShardStat.RunningCount.SetMax(int32(statistics.DefaultStat.RXTokenFillRate / 4))
	//statistics.DefaultStat.RebuildShardStat.DownloadingCount.SetMax(int32(statistics.DefaultStat.RXTokenFillRate / 4))

	log.Println("距离上次启动", time.Now().Sub(lt), time.Duration(config.Gconfig.BanTime)*time.Second)

	TokenPool.Utp().Save()
	TokenPool.Dtp().Save()
	msg.Other = fmt.Sprintf("[%s]", statistics.DefaultStat.String())
	log.Println("[report] other:", msg.Other)

	log.Printf("[report] msg.addr [%v]   msg.addrsCmd [%v]\n", msg.Addrs, msg.AddrsCmd)

	msg.Rebuilding = rce.Len()

	resData, err := proto.Marshal(&msg)
	log.Printf("RX:%d,TX:%d\n", msg.Rx, msg.Tx)
	log.Printf("cpu:%d%% mem:%d%% max-space: %d block\n", msg.Cpu, msg.Memory, msg.MaxDataSpace)
	log.Printf("data Hash %s\n", msg.Hash)
	if err != nil {
		log.Println("send report msg fail:", err)
	}

	log.Printf("Report to %s:%v\n", bp.ID, bp.Addrs)
	res, err := sn.SendBPMsg(sn.GetBP(), message.MsgIDStatusRepReq.Value(), resData)
	if err != nil {
		log.Println("Send report msg fail:", err)
	} else {
		var resMsg message.StatusRepResp
		log.Printf("report res len is %d\n", len(res))
		proto.Unmarshal(res[2:], &resMsg)
		if resMsg.ProductiveSpace >= 0 {
			sn.owner.BuySpace = uint64(resMsg.ProductiveSpace)
			diskHash.CopyHead()

			//重新上报成功了就应该重新开启？？？？
			disableReport = false
			TokenPool.Utp().Start()
			randDownload.Start()
		} else {
			log.Printf("report error %d\n", resMsg.ProductiveSpace)
			switch resMsg.ProductiveSpace {
			case -1:
				disableReport = true
			case -2:
				return
			case -7:
				TokenPool.Utp().Stop()
				randDownload.Stop()
			case -8:
				// 可采购空间不足
				TokenPool.Utp().Stop()
			case -12:
				log.Printf("采购空间出错\n")
			}
		}
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

func InitTokenPool(s *statistics.Stat) {
	cfg := config.Gconfig
	TokenPool.UploadTP.GetRate = s.RXTest.GetRate
	TokenPool.DownloadTP.GetRate = s.TXTest.GetRate
	go TokenPool.UploadTP.AutoChangeTokenInterval(cfg.RXIncreaseThreshold, cfg.Increase, cfg.RXDecreaseThreshold, cfg.Decrease)
	go TokenPool.DownloadTP.AutoChangeTokenInterval(cfg.TXIncreaseThreshold, cfg.Increase, cfg.TXDecreaseThreshold, cfg.Decrease)
}
