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

type LRCEngine struct {
	lrc      lrcpkg.Shardsinfo
	GetShard GetShardFunc
}

func NewLRCEngine(gsfunc GetShardFunc) *LRCEngine {
	var le LRCEngine
	le.lrc = lrcpkg.Shardsinfo{}

	le.GetShard = gsfunc

	le.lrc.LRCinit(13)

	return &le
}

type LRCHandler struct {
	le     *LRCEngine
	si     *lrcpkg.Shardsinfo
	shards [][]byte
}

func (le *LRCEngine) GetLRCHandler(shardsinfo *lrcpkg.Shardsinfo) (*LRCHandler, error) {
	lrch := LRCHandler{le, shardsinfo, nil}
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
start:
	lrch.shards = make([][]byte, 0)
	n++
	log.Println("尝试第", n, "次")

	sl, _ := lrch.le.lrc.GetNeededShardList(lrch.si.Handle)

	var number int
	var indexs []int16
	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

	log.Println("[recover]need shard list", indexs, len(indexs))

	k := 0
	for _, idx := range indexs {
		k++

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		peer := td.Locations[idx]
		shard, err := lrch.le.GetShard(ctx, peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number)

		// if there is some error, we should to try again
		if err != nil {
			fmt.Println("[recover]first getshard error:",err)
			shard, err = lrch.le.GetShard(ctx, peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number)
            if err != nil || len(shard) == 0 {
            	fmt.Println("[recover]second getshard error:",err,"len shard=",len(shard))
					continue
            }
		}

		if len(shard) == 0 {
			log.Println("[ytlrc] shard is empty!!")
			continue
		}

		status := lrch.le.lrc.AddShardData(lrch.si.Handle, shard)
		if status > 0{
			data, status2 := lrch.le.lrc.GetRebuildData(lrch.si)
			if status2 > 0 {        //rebuild success
				return data, nil
			}
		}else if status < 0 {     //rebuild failed
			if n < 3 {
				goto start
			}
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