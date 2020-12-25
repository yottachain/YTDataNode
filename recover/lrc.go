package recover

import (
	"context"
	"fmt"
	"github.com/mr-tron/base58"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	lrcpkg "github.com/yottachain/YTLRC"
	"strings"
	"time"
)

type GetShardFunc func(ctx context.Context, id string, taskID string, addrs []string, hash []byte, n *int) ([]byte, error)

type GetShardFuncLrc func(id string, taskID string, addrs []string, hash []byte, n *int,sw *Switchcnt, tasklife int32) ([]byte, error)

type IncRbdSuccCnt func(n uint16)


type LRCEngine struct {
	lrc       lrcpkg.Shardsinfo
	GetShard  GetShardFuncLrc
	IncRbdSucc  IncRbdSuccCnt
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
	swget    int8
	swconn   int8
	swtoken  int8
	swshard  int8
}

type LRCHandler struct {
	le     *LRCEngine
	si     *lrcpkg.Shardsinfo
	shards [][]byte
}

func (le *LRCEngine) GetLRCHandler(shardsinfo *lrcpkg.Shardsinfo) (*LRCHandler, error) {
	lrch := LRCHandler{le:le, si:shardsinfo, shards:nil}
	if h := le.lrc.GetRCHandle(shardsinfo); h == nil {
		return nil, fmt.Errorf("LRC get handler failed")
	}
	return &lrch, nil
}

func (lrch *LRCHandler) Recover(td message.TaskDescription, pkgstart time.Time, tasklife int32) ([]byte, error) {
	defer lrch.si.FreeHandle()

	log.Printf("[recover]lost idx %d\n", lrch.si.Lostindex)
	defer log.Printf("[recover]recover idx end %d\n", lrch.si.Lostindex)
	var n uint16
	var shard []byte
	var err error

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
	var efforttms  int8 = 30

	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

effortwk:
	if len(indexs2) > 0 && effortsw > 0{
		indexs = indexs[0:0]
		indexs = indexs2[:]
		indexs2 = indexs2[0:0]
	}

	//log.Println("[recover]need shard list", indexs, len(indexs))

	k := 0
	for _, idx := range indexs {
		k++
		peer := td.Locations[idx]
        if lrch.si.ShardExist[idx] == 0{
        	log.Println("[recover] shard_not_online, cannot get the shard,idx=",idx)
        	continue
		}

		log.Println("[recover] shard_online, get the shard,idx=",idx)

		if time.Now().Sub(pkgstart).Seconds() > float64(tasklife)-65 {
			log.Println("[recover] rebuild time expired! spendtime=",time.Now().Sub(pkgstart).Seconds(),"taskid=",BytesToInt64(td.Id[0:8]))
			//logelk:=re.MakeReportLog(peer.NodeId,td.Hashs[idx],"timeOut",err)
			//go re.reportLog(logelk)
			return nil, fmt.Errorf("rebuild data failed, time expired")
		}

		sw := Switchcnt{0,0,0,0}

		//for{
		shard, err = lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number, &sw, tasklife)

		if err != nil{
			log.Println("[recover][optimize] Get data Slice fail,idx=",idx,err.Error())

			if (strings.Contains(err.Error(),"Get data Slice fail")){
				continue
			}

			if(strings.Contains(err.Error(),"version is too low")){
				continue
			}

			indexs2 = append(indexs2, idx)
			continue
		}

		if len(shard) < 1 {
			log.Println("[recover][ytlrc] shard is empty or get error!! idx=",idx)
			indexs2 = append(indexs2, idx)
			continue
		}

		if ! message.VerifyVHF(shard, td.Hashs[idx]) {
			log.Println("[recover] shard_verify_failed! idx=",idx,"shardindex=",shard[0],"reqVHF=",base58.Encode(td.Hashs[idx]), "shardVHF=",base58.Encode(message.CaculateHash(shard)))
			continue
		}

		if time.Now().Sub(pkgstart).Seconds() > float64(tasklife)-63{
			log.Println("[recover] rebuild time expired! spendtime=",time.Now().Sub(pkgstart).Seconds(),"taskid=",BytesToInt64(td.Id[0:8]))
			return nil, fmt.Errorf("rebuild data failed, time expired")
		}//rebuild success

		status := lrch.si.AddShardData(lrch.si.Handle, shard)
		if status > 0{
			data, status2 := lrch.si.GetRebuildData(lrch.si)
			if status2 > 0 {
                lrch.le.IncRbdSucc(n)
				//log.Println("[recover] check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
				return data, nil
			}
		}else if status < 0 {     //rebuild failed
			log.Println("[recover] low_level_lrc status=",status)
		}
	}

	if time.Now().Sub(pkgstart).Seconds() > float64(tasklife)-62 {
		log.Println("[recover] rebuild time expired! spendtime=",time.Now().Sub(pkgstart).Seconds(),"taskid=",BytesToInt64(td.Id[0:8]))
		return nil, fmt.Errorf("rebuild data failed, time expired")
	}

	efforttms--

	if efforttms <= 0 && n < 3{
		indexs2 = indexs2[0:0]
		effortsw = 0
		goto start
		return nil, fmt.Errorf("rebuild data failed, efforttms goto zero")
	}

	if len(indexs2) > 0{
		effortsw = 1
		goto effortwk
	}
	return nil, fmt.Errorf("rebuild data failed")
}

func (lrch *LRCHandler) GetShards() [][]byte {
	return lrch.shards
}