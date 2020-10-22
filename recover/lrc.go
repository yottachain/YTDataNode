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

func (lrch *LRCHandler) Recover(td message.TaskDescription) ([]byte, error) {
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
	log.Println("尝试第", n, "次")

	sl, _ := lrch.si.GetNeededShardList(lrch.si.Handle)

	var number int
	var indexs []int16
	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
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
		retrytimes := 20
		log.Println("[recover] shard_online, get the shard,idx=",idx)

		sw := Switchcnt{0,0,0}

		for{
			shard, err = lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number,&sw)
			if err == nil && len(shard) > 0 {
				break
			}

			retrytimes--

			if 0 >= retrytimes{
				break
			}
			<-time.After(time.Millisecond * 500)
			//if time.Now().Sub(getshdstart).Seconds() > 30{
			//	break
			//}
		}

		if len(shard) == 0 || err != nil {
			log.Println("[recover][ytlrc] shard is empty!!")
			if k >= len(indexs) && n < 3 {
				goto  start
			}
			continue
		}

		status := lrch.si.AddShardData(lrch.si.Handle, shard)
		if status > 0{
			data, status2 := lrch.si.GetRebuildData(lrch.si)
			if status2 > 0 {        //rebuild success
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

	return nil, fmt.Errorf("rebuild data failed")
}

func (lrch *LRCHandler) GetShards() [][]byte {
	return lrch.shards
}