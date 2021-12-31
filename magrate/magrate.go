package magrate

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	log "github.com/yottachain/YTDataNode/logger"
	ytfs "github.com/yottachain/YTFS"
	ydcommon "github.com/yottachain/YTFS/common"
	"io/ioutil"
	"net/http"
	"sync"
)

type Mr struct {
	sync.Mutex
}

type minerKeys struct {
	shardKeys []string
}

const MrDataKey = "magrate_data_key_"

func NewMr() *Mr{
	return new(Mr)
}


//while magrate don't write data
func (mr *Mr)Run(ytfs *ytfs.YTFS, isRocks bool, minerId uint32) error {
	if !isRocks {
		return nil
	}

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

	url := fmt.Sprintf("http://150.138.84.46:22222/node_shards?minerid=%d", minerId)
	log.Println("[magrate] url:", url)

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("%s", err.Error())
	}

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var minerShardKeys minerKeys

	err = json.Unmarshal(resBuf, &minerShardKeys.shardKeys)
	if err != nil {
		return err
	}

	snShardMap := make(map[string]struct{}, 0)
	for _, v := range minerShardKeys.shardKeys {
		log.Printf("[magrate] shard key: %s\n", v)
		snShardMap[v] = struct{}{}
	}

	ytfs.YtfsDB().TravelDB(func(key, value []byte) error {
		dataPos := binary.LittleEndian.Uint32(value)
		curPos := ytfs.PosIdx()
		Hkey := ydcommon.IndexTableKey(ydcommon.BytesToHash(key))

		//in sn database? yes magrate, else del
		if _, ok := snShardMap[base58.Encode(key)]; ok {
			if uint64(dataPos) > curPos {
				shard, err := ytfs.Get(Hkey)
				if err != nil {
					log.Printf("[magrate] get hash err:%s, key:%s\n",
						err.Error(), base58.Encode(key))
					return err
				}

				kvMap :=  map[ydcommon.IndexTableKey][]byte{Hkey:shard}
				_, err = ytfs.BatchPutNormal(kvMap)
				//err = ytfs.Put(Hkey, shard)
				hash := md5.Sum(shard)
				if err != nil {
					log.Printf("[magrate] put hash err:%s, key:%s, shard:%s\n",
						err.Error(), base58.Encode(key), base58.Encode(hash[:]))
					return err
				}

				log.Printf("[magrate] succs key:%s, shard:%s, before pos %d, after pod %d\n",
					base58.Encode(key), base58.Encode(hash[:]), dataPos, curPos)
			}
		}else {
			//del if not exist
			_ = ytfs.YtfsDB().Delete(Hkey)
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
