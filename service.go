package node

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yottachain/YTDataNode/TokenPool"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/diskHash"
	"github.com/yottachain/YTDataNode/randDownload"
	"github.com/yottachain/YTDataNode/statistics"

	"github.com/gogo/protobuf/proto"
	"github.com/multiformats/go-multiaddr"

	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/service"
	ytfs "github.com/yottachain/YTFS"
)

type ytfsDisk *ytfs.YTFS

var rms *service.RelayManager
var lt = (&statistics.LastUpTime{}).Read()
var disableReport = false

func (sn *storageNode) Service() {

	statistics.DefaultStat.TokenQueueLen = 200

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
	msg.Addrs = sn.Addrs()

	msg.Cpu = sn.Runtime().AvCPU
	msg.Memory = sn.Runtime().Mem
	msg.Id = sn.Config().IndexID
	msg.MaxDataSpace = sn.Config().Storages[0].StorageVolume / 16384
	msg.UsedSpace = sn.Config().Storages[0].StorageVolume / 16384
	msg.RealSpace = uint32(sn.Config().Storages[0].StorageVolume) / 16384
	msg.AllocSpace = sn.Config().AllocSpace / 16384

	msg.Relay = sn.config.Relay
	msg.Version = sn.config.Version()
	msg.Rx = GetXX("R")
	msg.Tx = GetXX("T")

	hash, err := diskHash.GetHash()
	if err == nil {
		msg.Hash = hash
	} else {
		log.Println("[diskHash]", err.Error())
	}

	statistics.DefaultStat.Lock()
	statistics.DefaultStat.AvailableTokenNumber = 0
	statistics.DefaultStat.UseKvDb = sn.config.UseKvDb
	statistics.DefaultStat.RXTokenFillRate = 500
	statistics.DefaultStat.TXTokenFillRate = 500

	statistics.DefaultStat.Connection = sn.Host().ConnStat().GetSerconnCount() + sn.Host().ConnStat().GetCliconnCount()

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

	log.Println("距离上次启动", time.Now().Sub(lt), time.Duration(config.Gconfig.BanTime)*time.Second)

	TokenPool.Utp().Save()
	TokenPool.Dtp().Save()
	msg.Other = fmt.Sprintf("[%s]", statistics.DefaultStat.String())
	log.Println("[report] other:", msg.Other)

	msg.Rebuilding = 0

	resData, err := proto.Marshal(&msg)
	log.Printf("RX:%d,TX:%d\n", msg.Rx, msg.Tx)
	log.Printf("cpu:%d%% mem:%d%% max-space: %d block\n", msg.Cpu, msg.Memory, msg.MaxDataSpace)
	log.Printf("data Hash %s\n", msg.Hash)
	if err != nil {
		log.Println("send report msg fail:", err)
	}

	log.Printf("Report to %s:%v\n", bp.ID, bp.Addrs)
	//res, err := clt.SendMsg(ctx, message.MsgIDStatusRepReq.Value(), resData)
	res, err := sn.SendBPMsg(sn.GetBP(), message.MsgIDStatusRepReq.Value(), resData)
	//defer clt.Close()
	if err != nil {
		log.Println("Send report msg fail:", err)
	} else {
		var resMsg message.StatusRepResp
		proto.Unmarshal(res[2:], &resMsg)
		if resMsg.ProductiveSpace >= 0 {
			sn.owner.BuySpace = uint64(resMsg.ProductiveSpace)
		} else {
			log.Printf("report error %d\n", resMsg.ProductiveSpace)
			switch resMsg.ProductiveSpace {
			case -1:
				disableReport = true
			case -2:
				//log.Println("[report] error")
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

}
