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

type GetShardFuncLrc func(id string, taskID string, addrs []string, hash []byte, n *int,sw *Switchcnt) ([]byte, error)

type LRCEngine struct {
	lrc      lrcpkg.Shardsinfo
	GetShard GetShardFuncLrc
}

func NewLRCEngine(gsfunc GetShardFuncLrc) *LRCEngine {
	var le LRCEngine
	le.lrc = lrcpkg.Shardsinfo{}

	le.GetShard = gsfunc

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

func (lrch *LRCHandler) Recover(td message.TaskDescription, pkgstart time.Time) ([]byte, error) {
	defer lrch.si.FreeHandle()

	log.Printf("[recover]lost idx %d\n", lrch.si.Lostindex)
	defer log.Printf("[recover]recover idx end %d\n", lrch.si.Lostindex)
	var n uint16
	var shard []byte
	var err error
	var effortsw int8 = 0
	var efforttms  int8 = 30

start:
	lrch.shards = make([][]byte, 0)
	//
	n++
	log.Println("[recover] retry", n, "times")

	sl, _ := lrch.si.GetNeededShardList(lrch.si.Handle)

	var number int
	var indexs []int16
	var indexs2 []int16

	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

effortwk:
	if len(indexs2) > 0 && effortsw > 0{
		indexs = indexs[0:0]
		log.Println("[recover][optimize][1] indexs=",indexs," indexs2=",indexs2)
		indexs = indexs2[:]
		log.Println("[recover][optimize][2] indexs=",indexs," indexs2=",indexs2)
		indexs2 = indexs2[0:0]
		log.Println("[recover][optimize][3] indexs=",indexs," indexs2=",indexs2)
	}

	log.Println("[recover]need shard list", indexs, len(indexs))
	log.Println("[recover] ShardExist=",lrch.si.ShardExist)
	k := 0
	for _, idx := range indexs {
		k++
		peer := td.Locations[idx]
        if lrch.si.ShardExist[idx] == 0{
        	log.Println("[recover] shard_not_online, cannot get the shard,idx=",idx)
        	continue
		}

		//getshdstart := time.Now()
		//retrytimes := 20
		log.Println("[recover] shard_online, get the shard,idx=",idx)

		//if time.Now().Sub(pkgstart).Seconds() > 1800-60 {
		//	log.Println("[recover] rebuild time expired! spendtime=",)
		//
		//	//return nil, fmt.Errorf("rebuild data failed, time expired")
		//}

		sw := Switchcnt{0,0,0,0}

		//for{
		shard, err = lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number,&sw)

		if err != nil{
			log.Println("[recover][optimize] Get data Slice fail,idx=",idx,err.Error())
			if k >= len(indexs) && n < 3 {
				goto  start
			}

			if (strings.Contains(err.Error(),"Get data Slice fail")){
				//log.Println("[recover][optimize] Get data Slice fail, shard not exist")
				continue
					//break
			}

			if n >= 3{
				indexs2 = append(indexs2, idx)
			}
			continue
		}

		if len(shard) == 0 {
			log.Println("[recover][ytlrc] shard is empty or get error!! idx=",idx)
			if k >= len(indexs) && n < 3 {
				goto  start
			}

			if n >= 3{
				indexs2 = append(indexs2, idx)
			}
			continue
		}

		if ! message.VerifyVHF(shard, td.Hashs[idx]) {
			log.Println("[recover] shard_verify_failed! idx=",idx,"shardindex=",shard[0],"reqVHF=",base58.Encode(td.Hashs[idx]), "shardVHF=",base58.Encode(message.CaculateHash(shard)))
			if k >= len(indexs) && n < 3 {
				goto  start
			}

			continue
			//break
		}

		//if len(shard) > 0 {
		//
		//}

			//retrytimes--
			//
			//if 0 >= retrytimes{
			//	//break
			//}
			//<-time.After(time.Millisecond * 500)
		//}



		//if time.Now().Sub(pkgstart).Seconds() > 1800-60{
		//	log.Println("[recover] rebuild time expired!")
		//	return nil, fmt.Errorf("rebuild data failed, time expired")
		//}

		status := lrch.si.AddShardData(lrch.si.Handle, shard)

		if status > 0{
			data, status2 := lrch.si.GetRebuildData(lrch.si)
			if status2 > 0 {        //rebuild success
				log.Println("[recover] check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
				return data, nil
			}
		}else if status < 0 {     //rebuild failed
			if n < 3 {
                log.Println("[recover] low_level_lrc status=",status)
				goto start
			}
			log.Println("[recover] low_level_lrc status=",status)
		}else {
			if k >= len(indexs) && n < 3 {  //rebuild mode(hor, ver) over
				goto start
			}
		}
	}

	//if time.Now().Sub(pkgstart).Seconds() > 1800-60 {
	//	log.Println("[recover] rebuild time expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
	//	return nil, fmt.Errorf("rebuild data failed, time expired")
	//}

	efforttms--

	if efforttms <= 0 {
		return nil, fmt.Errorf("rebuild data failed, efforttms goto zero")
	}

	if len(indexs2) > 0{
		effortsw = 1
		goto effortwk
	}
	log.Println("[recover] check_rebuild_time_expired! spendtime=",time.Now().Sub(pkgstart).Seconds())
	return nil, fmt.Errorf("rebuild data failed")
}

func (lrch *LRCHandler) GetShards() [][]byte {
	return lrch.shards
}