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
	le *LRCEngine
	si *lrcpkg.Shardsinfo
}

func (le *LRCEngine) GetLRCHandler(shardsinfo *lrcpkg.Shardsinfo) (*LRCHandler, error) {
	lrch := LRCHandler{le, shardsinfo}
	if h := le.lrc.GetRCHandle(shardsinfo); h == nil {
		return nil, fmt.Errorf("LRC get handler failed")
	}

	return &lrch, nil
}

func (lrch *LRCHandler) Recover(td message.TaskDescription) ([]byte, error) {
	defer lrch.le.lrc.FreeHandle(lrch.si)

	log.Printf("[recover]lost idx %d\n", lrch.si.Lostindex)
	defer log.Printf("[recover]recover idx end %d\n", lrch.si.Lostindex)
	var n byte
start:
	n++
	log.Println("尝试第", n, "次")

	sl, _ := lrch.le.lrc.GetNeededShardList(lrch.si.Handle)

	var number int
	var indexs []int16
	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

	log.Println("[recover]need shard list", indexs)

	for _, idx := range indexs {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		peer := td.Locations[idx]
		shard, err := lrch.le.GetShard(ctx, peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number)
		if err != nil {
			// todo: retry
			continue
		} else {
			if status := lrch.le.lrc.AddShardData(lrch.si.Handle, shard); status < 0 {
				goto start
			}
		}
	}

	data, status := lrch.le.lrc.GetRebuildData(lrch.si)

	if status > 0 {
		return data, nil
	} else {
		return nil, fmt.Errorf("rebuild data failed %d", status)
	}
}
