package recover

import (
	"context"
	"fmt"
	"github.com/mr-tron/base58"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	lrcpkg "github.com/yottachain/YTLRC"
	"time"
)

type GetShardFunc func(ctx context.Context, id string, taskID string, addrs []string, hash []byte, n *int) ([]byte, error)

type GetShardFuncLrc func(id string, taskID string, addrs []string, hash []byte, n *int, sw *Switchcnt, tasklife int32) ([]byte, error)

type IncRbdSuccCnt func(n uint16)

type LRCEngine struct {
	lrc        lrcpkg.Shardsinfo
	GetShard   GetShardFuncLrc
	IncRbdSucc IncRbdSuccCnt
}

func NewLRCEngine(gsfunc GetShardFuncLrc, incrbdsucc IncRbdSuccCnt) *LRCEngine {
	var le LRCEngine
	le.lrc = lrcpkg.Shardsinfo{}

	le.GetShard = gsfunc

	le.IncRbdSucc = incrbdsucc

	le.lrc.LRCinit(13)

	return &le
}

type Switchcnt struct {
	swget   int8
	swconn  int8
	swtoken int8
	swshard int8
}

type LRCHandler struct {
	le     *LRCEngine
	si     *lrcpkg.Shardsinfo
	shards [][]byte
}

func (le *LRCEngine) GetLRCHandler(shardsinfo *lrcpkg.Shardsinfo) (*LRCHandler, error) {
	lrch := LRCHandler{le: le, si: shardsinfo, shards: nil}
	if h := le.lrc.GetRCHandle(shardsinfo); h == nil {
		return nil, fmt.Errorf("LRC get handler failed")
	}
	return &lrch, nil
}

func (lrch *LRCHandler) RecoverOrigShard(shdinfo *lrcpkg.Shardsinfo, lostidx uint16, stage uint8, td message.TaskDescription, num *int, sw *Switchcnt, tasklife int32) ([]byte, []int16, error) {
	var err error
	log.Println("[recover] recover orig miss shard missidx=", lostidx)
	if stage < 0 || stage > 1 {
		log.Println("[recover]error: stage out of range, missidx=", lostidx)
		err = fmt.Errorf("[recover][error] stage out of range, the value should 0 or 1 only")
		return nil, nil, err
	}

	if nil == shdinfo || nil == shdinfo.Handle {
		log.Println("[recover]error: shdinfo.handle is nil, missidx=", lostidx)
		err = fmt.Errorf("shdinfo.handle is nil")
		return nil, nil, err
	}

	rcvlostidx, firststage, err := shdinfo.GetHandleParam(shdinfo.Handle)
	if nil != err {
		log.Println("[recover] error: GetHandleParam failed,", err, "missidx=", lostidx)
		return nil, nil, err
	}

	if lostidx < 128 {
		err = shdinfo.SetHandleParam(shdinfo.Handle, uint8(lostidx), stage)
		if err != nil {
			shdinfo.SetHandleParam(shdinfo.Handle, uint8(rcvlostidx), uint8(firststage))
			fmt.Printf("[recover] error: modify handle param:%s missidx=%d, stage=%d\n", err.Error(), lostidx, 0)
			return nil, nil, err
		}
	} else {
		fmt.Println("[recover] error: missidx out of range, missidx=", lostidx)
		err = fmt.Errorf("[recover] missidx out of range, missidx=", lostidx)
		return nil, nil, err
	}

	datashard, missarr, err := lrch.RecoverShardStage(shdinfo, td, num, sw, tasklife)
	if err != nil {
		log.Println("[recover] error:", err, "missidx= ", lostidx)
		shdinfo.SetHandleParam(shdinfo.Handle, uint8(rcvlostidx), uint8(firststage))
		return nil, missarr, err
	}

	err = shdinfo.SetHandleParam(shdinfo.Handle, uint8(rcvlostidx), uint8(firststage))
	if err != nil {
		log.Println("[recover] error:", err, "missidx= ", lostidx)
		return nil, nil, err
	}
	log.Println("[recover] rebuild success missidx= ", lostidx)
	return datashard, missarr, err
}

func (lrch *LRCHandler) RecoverShardStage(shdinfo *lrcpkg.Shardsinfo, td message.TaskDescription, num *int, sw *Switchcnt, tasklife int32) ([]byte, []int16, error) {
	//recshards := make([][]byte, 0)
	var err error
	var shard []byte

	if nil == shdinfo || nil == shdinfo.Handle {
		err = fmt.Errorf("shdinfo.handle is nil")
		return nil, nil, err
	}

	sl, _ := shdinfo.GetNeededShardList(shdinfo.Handle)

	//var number int
	var indexs []int16
	var missarr []int16
	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

	log.Println("[recover] missrecover need shard list", indexs, len(indexs))

	for _, idx := range indexs {
		peer := td.Locations[idx]
		for r := 1; r < 6; r++ {
			shard, err = lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], num, sw, tasklife)
			if err == nil && len(shard) == 16384 {
				break
			}

			if 5 == r {
				fmt.Println("[recover] error: get shard error, missidx=", idx)
				missarr = append(missarr, idx)
			}
			<-time.After(time.Millisecond * 50)
		}

		if len(shard) != 16384 {
			log.Println("[recover] error: shard lenth != 16K, missidx=", idx)
			continue
		}

		status, err := shdinfo.AddShardData(shdinfo.Handle, shard)

		if err != nil {
			fmt.Println(err)
			if status == -100 {
				return nil, nil, err
			}
			continue
		}
		//log.Println("[recover] status=",status)
		if status > 0 {
			rcvdata, status2 := shdinfo.GetRebuildData(shdinfo)
			if status2 > 0 { //rebuild success
				missidx, _, _ := shdinfo.GetHandleParam(shdinfo.Handle)
				fmt.Println("[recover] recover success shard, missidx=", missidx)
				return rcvdata, nil, nil
			}
		} else if status < 0 { //rebuild failed
			//do some process later
		} else {
			//do some process later
		}
	}
	err = fmt.Errorf("recover data failed")
	return nil, missarr, err
}

func (lrch *LRCHandler) Recover(td message.TaskDescription, pkgstart time.Time, tasklife int32) ([]byte, error) {
	defer lrch.si.FreeHandle()

	log.Printf("[recover]lost idx %d\n", lrch.si.Lostindex)
	defer log.Printf("[recover]recover idx end %d\n", lrch.si.Lostindex)
	var n uint16
	var shard []byte
	var err, err2 error

start:
	lrch.shards = make([][]byte, 0)
	//
	n++
	log.Println("[recover] retry", n, "times")

	sl, _ := lrch.si.GetNeededShardList(lrch.si.Handle)

	var number int
	var indexs []int16
	var indexs2 []int16
	var effortsw int8 = 0
	//var efforttms int8 = 30

	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

effortwk:
	if len(indexs2) > 0 && effortsw > 0 {
		indexs = indexs[0:0]
		indexs = indexs2[:]
		indexs2 = indexs2[0:0]
	}

	//log.Println("[recover]need shard list", indexs, len(indexs))

	//k := 0
	for _, idx := range indexs {
		//k++
		peer := td.Locations[idx]
		sw := Switchcnt{0, 0, 0, 0}
		//if lrch.si.ShardExist[idx] == 0 {
		//	log.Println("[recover] shard_not_online, cannot get the shard,idx=", idx)
		//	shard, _, err2 = lrch.RecoverOrigShard(lrch.si, uint16(idx), uint8(n), td, &number, &sw, tasklife)
		//	if err2 != nil {
		//		log.Println("[recover] rebuild miss shard error:", err, " idx=", idx)
		//		indexs2 = append(indexs2, idx)
		//		continue
		//	}
		//	lrch.le.IncRbdSucc(0)
		//} else {
		// 过期时间 1小时
		//outtimer := time.After(time.Hour)
		log.Println("[recover] shard_online, get the shard,idx=", idx)

		if time.Now().Sub(pkgstart).Seconds() > float64(tasklife)-65 {
			log.Println("[recover] rebuild time expired! spendtime=", time.Now().Sub(pkgstart).Seconds(), "taskid=", BytesToInt64(td.Id[0:8]))
			//logelk:=re.MakeReportLog(peer.NodeId,td.Hashs[idx],"timeOut",err)
			//go re.reportLog(logelk)
			return nil, fmt.Errorf("rebuild data failed, time expired")
		}

		shard, err = lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number, &sw, tasklife)

		if err != nil {
			log.Println("[recover][optimize] Get data Slice fail,idx=", idx, err.Error())

			shard, _, err2 = lrch.RecoverOrigShard(lrch.si, uint16(idx), uint8(n), td, &number, &sw, tasklife)
			if err2 != nil {
				log.Println("[recover] rebuild miss shard error:", err, " idx=", idx)
				//if strings.Contains(err.Error(), "Get data Slice fail") {
				//	continue
				//}
				//
				//if strings.Contains(err.Error(), "version is too low") {
				//	continue
				//}

				indexs2 = append(indexs2, idx)
				continue
			}
			lrch.le.IncRbdSucc(0)
		}

		//out := false
		//
		//select {
		//case <-outtimer:
		//	out = true
		//default:
		//	continue
		//}
		//if out {
		//	break
		//}
		//}
		//}

		if len(shard) < 16384 {
			log.Println("[recover][ytlrc] shard is empty or get error!! idx=", idx)
			indexs2 = append(indexs2, idx)
			continue
		}

		if !message.VerifyVHF(shard, td.Hashs[idx]) {
			log.Println("[recover] shard_verify_failed! idx=", idx, "shardindex=", shard[0], "reqVHF=", base58.Encode(td.Hashs[idx]), "shardVHF=", base58.Encode(message.CaculateHash(shard)))
			indexs2 = append(indexs2, idx)
			continue
		}

		if time.Now().Sub(pkgstart).Seconds() > float64(tasklife)-63 {
			log.Println("[recover] rebuild time expired! spendtime=", time.Now().Sub(pkgstart).Seconds(), "taskid=", BytesToInt64(td.Id[0:8]))
			return nil, fmt.Errorf("rebuild data failed, time expired")
		} //rebuild success
		status, err := lrch.si.AddShardData(lrch.si.Handle, shard)
		if err != nil {
			log.Println("[recover] mainrecover error: ", err)
			continue
		}
		if status > 0 {
			data, status2 := lrch.si.GetRebuildData(lrch.si)
			if status2 > 0 {
				lrch.le.IncRbdSucc(n)
				//log.Println("[recover] check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
				return data, nil
			}
		} else if status < 0 { //rebuild failed
			log.Println("[recover] low_level_lrc status=", status)
		}
	}

	if time.Now().Sub(pkgstart).Seconds() > float64(tasklife)-62 {
		log.Println("[recover] rebuild time expired! spendtime=", time.Now().Sub(pkgstart).Seconds(), "taskid=", BytesToInt64(td.Id[0:8]))
		return nil, fmt.Errorf("rebuild data failed, time expired")
	}

	// efforttms--

	if n < 3 {
		indexs2 = indexs2[0:0]
		effortsw = 0
		goto start
	}
	//return nil, fmt.Errorf("rebuild data failed, efforttms goto zero")
	fmt.Println("[recover] len(indexs2)=", len(indexs2))
	if len(indexs2) > 0 {
		effortsw = 1
		goto effortwk
	}

	return nil, fmt.Errorf("rebuild data failed")
}

func (lrch *LRCHandler) GetShards() [][]byte {
	return lrch.shards
}
