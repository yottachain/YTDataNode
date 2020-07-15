package main

import (
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTFS/storage"
	"github.com/tecbot/gorocksdb"
	ydcommon "github.com/yottachain/YTFS/common"
	"log"
	"os"
	"path"
	"sync/atomic"
	"time"
	"encoding/binary"

)

var num uint64
var preNum uint64
var cfg *config.Config
var end chan struct{}
var ti *storage.TableIterator
var preTime time.Time
var glbti storage.TableIterator

var Mdb *KvDB

var mdbFileName = "/maindb"
var ytPosKey    = "yt_rocks_pos_key"
var ytBlkSzKey  = "yt_blk_size_key"

type KvDB struct {
	Rdb *gorocksdb.DB
	ro  *gorocksdb.ReadOptions
	wo  *gorocksdb.WriteOptions
	PosKey ydcommon.IndexTableKey
	PosIdx ydcommon.IndexTableValue
}

func setKVtoDB(mdb *KvDB, key string, value uint32) error {
	HKey := ydcommon.BytesToHash([]byte(key))
	PosKey := ydcommon.IndexTableKey(HKey)

	valbuf := make([]byte,4)

	log.Println("setKVtoDB key=",key," value=",value)
	binary.LittleEndian.PutUint32(valbuf, value)
	err := mdb.Rdb.Put(mdb.wo,PosKey[:],valbuf)
	if err != nil {
		log.Println("update write pos to metadatafile err:", err)
	}
	return err
}

func openKVDB(DBPath string) (kvdb *KvDB, err error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, DBPath)
	if err != nil {
		log.Println("open rocksdb error")
		return nil, err
	}

	// 创建输入输出流
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	return &KvDB{
		Rdb   :  db,
		ro    :  ro,
		wo    :  wo,
	}, err
}

func init(){
	preTime = time.Now()
	end = make(chan struct{})
	cfg,_=config.ReadConfig()
	pathname := path.Join(util.GetYTFSPath(),"index.db")
	cfg.Options.UseKvDb = true
	ti,_=storage.GetTableIterator(pathname,cfg.Options)

	mdb,err := openKVDB(path.Join(util.GetYTFSPath(),"maindb"))
	if err != nil {
		log.Println(" open rocksdb err!!")
		panic(err)
	}

    Mdb = mdb
    Pos := ti.Len()
    err = setKVtoDB(mdb,ytPosKey,uint32(Pos))
	if err != nil {
		log.Println("set start Pos to DB err:",err)
		panic(err)
	}

	BlkSize := ti.BlockSize()
	err = setKVtoDB(mdb,ytBlkSzKey,BlkSize)
	log.Println("current blksize=",BlkSize)
	if err != nil {
		log.Println("set blocksize to DB err:",err)
		panic(err)
	}
}

func main(){
	go printSpeed()

	for {
		tb,err:=ti.GetNoNilTableBytes()
		if err !=nil {
			log.Println("get Table from indexdb err: ", err)
			break
		}

		for key,val := range tb {
			err = Mdb.Rdb.Put(Mdb.wo, key[:], val)
			if err != nil {
				log.Println("rbdkv_error")
			}
		}
		atomic.AddUint64(&num,uint64(len(tb)))
	}

	fileName := path.Join(util.GetYTFSPath(), "dbsafe")
	os.Create(fileName)

	log.Println("rbdkv_success")
	cfg.UseKvDb = true
	cfg.Save()
	end<- struct{}{}
}

func printSpeed () {
	for {
		select {
		case <-end:
			return
		default:
			<-time.After(time.Second)
			addCount:=atomic.LoadUint64(&num)-preNum
			if addCount == 0 {
				continue
			}
			log.Printf("copy %d 耗时 %f s进度 %d/%d 百分比 %.4f 预计完成剩余时间%s\n",
				addCount,
				time.Now().Sub(preTime).Seconds(),
				num,ti.Len(),
				float64(num)/(float64(ti.Len())+1)*100,
				FormatTd((ti.Len()-num)/addCount),
			)
			preNum = atomic.LoadUint64(&num)
			preTime = time.Now()
		}
	}
}

func FormatTd(td uint64) string {
	const (
		m = 60
		h = 60 * m
		d = 60 * 24
	)

	if td > d {
		return fmt.Sprintf("%d 天",td/d)
	}
	if td > h {
		return fmt.Sprintf("%d 时",td/h)
	}
	if td > m {
		return fmt.Sprintf("%d 分",td/m)
	}
	return fmt.Sprintf("%d 秒",td)
}
