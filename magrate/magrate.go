package magrate

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	log "github.com/yottachain/YTDataNode/logger"
	ytfs "github.com/yottachain/YTFS"
	ydcommon "github.com/yottachain/YTFS/common"
	"sync"
)

type Mr struct {
	sync.Mutex
}

const MrDataKey = "magrate_data_key_"

func NewMr() *Mr{
	return new(Mr)
}

//while magrate don't write data
func (mr *Mr)Run(ytfs *ytfs.YTFS) error {
	keyMr, err := ytfs.YtfsDB().GetDb([]byte(MrDataKey))
	if err != nil {
		_ = fmt.Errorf("[magrate] get magrate key err %s\n", err.Error())
		return err
	}
	if keyMr != nil {
		fmt.Println("[magrate] get magrate key have existed")
		return fmt.Errorf("magrate key have existed")
	}else {
		log.Println("[magrate] start")
	}

	ytfs.YtfsDB().TravelDB(func(key, value []byte) error {
		dataPos := binary.LittleEndian.Uint32(value)
		curPos := ytfs.PosIdx()
		if uint64(dataPos) > curPos {
			Hkey := ydcommon.IndexTableKey(ydcommon.BytesToHash(key))
			shard, err := ytfs.Get(Hkey)
			if err != nil {
				log.Printf("[magrate] get hash err:%s, key:%s\n",
					err.Error(), base58.Encode(key))
				return err
			}

			err = ytfs.Put(Hkey, shard)
			hash := md5.Sum(shard)
			if err != nil {
				log.Printf("[magrate] put hash err:%s, key:%s, shard:%s\n",
					err.Error(), base58.Encode(key), base58.Encode(hash[:]))
				return err
			}

			log.Printf("[magrate] succs key:%s, shard:%s, before pos %d, after pod %d\n",
				base58.Encode(key), base58.Encode(hash[:]), dataPos, curPos)
		}
		return nil
	})

	err = ytfs.YtfsDB().PutDb([]byte(MrDataKey), []byte("magrate_first"))
	if err != nil {
		log.Println("[magrate] put key fail")
		return fmt.Errorf("[magrate] put key fail")
	}else {
		log.Println("[magrate] put key succs")
	}

	return nil
}
