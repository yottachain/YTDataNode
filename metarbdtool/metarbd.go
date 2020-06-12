package main

import (
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTFS/storage"
	"github.com/tecbot/gorocksdb"
	ydcommon "github.com/yottachain/YTFS/common"
	"log"
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

	fmt.Println("setKVtoDB key=",key," value=",value)
	binary.LittleEndian.PutUint32(valbuf, value)
	err := mdb.Rdb.Put(mdb.wo,PosKey[:],valbuf)
	if err != nil {
		fmt.Println("update write pos to metadatafile err:", err)
	}
	return err
}

func openKVDB(DBPath string) (kvdb *KvDB, err error) {
	// 使用 gorocksdb 连接 RocksDB
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	// 设置输入目标数据库文件（可自行配置，./db 为当前测试文件的目录下的 db 文件夹）
	db, err := gorocksdb.OpenDb(opts, DBPath)
	if err != nil {
		fmt.Println("[kvdb] open rocksdb error")
		return nil, err
	}

	// 创建输入输出流
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	//diskPoskey := ydcommon.BytesToHash([]byte(ytPosKey))
	//val,err := db.Get(ro,diskPoskey[:])
	//posIdx := binary.LittleEndian.Uint32(val.Data())
	return &KvDB{
		Rdb   :  db,
		ro    :  ro,
		wo    :  wo,
		//PosKey:  ydcommon.IndexTableKey(diskPoskey),
		//PosIdx:  ydcommon.IndexTableValue(posIdx),
	}, err
}

func init(){
	preTime = time.Now()
	end = make(chan struct{})
	cfg,_=config.ReadConfig()
	pathname := path.Join(util.GetYTFSPath(),"index.db")
//	pathname2 := path.Join(util.GetYTFSPath(),"metadata.db")
//	fmt.Println("pathname2=",pathname2)
	ti,_=storage.GetTableIterator(pathname,cfg.Options)
//	ti,_=storage.GetTableIterator2(pathname1,pathname2,cfg.Options,glbti)
	fmt.Println("init  ti_pos=",ti.Len())
	fmt.Println("init  ti_blksize=",ti.BlockSize())
//	db,err := OpenFile(path.Join(util.GetYTFSPath(),"maindb"),nil)
	mdb,err := openKVDB(path.Join(util.GetYTFSPath(),"maindb"))
	if err != nil {
		fmt.Println("[rebuildkv] open rocksdb err!!")
		panic(err)
	}
    Mdb = mdb
    Pos := ti.Len()
    fmt.Println("[rebuildkv] current pos=",Pos)
    err = setKVtoDB(mdb,ytPosKey,uint32(Pos))
	if err != nil {
		fmt.Println("[rebuildkv] set start Pos to DB err:",err)
		panic(err)
	}
	BlkSize := ti.BlockSize()
	err = setKVtoDB(mdb,ytBlkSzKey,BlkSize)
	fmt.Println("[rebuildkv] current blksize=",BlkSize)
	if err != nil {
		fmt.Println("[rebuildkv] set blocksize to DB err:",err)
		panic(err)
	}
}

func main(){
	go printSpeed()
	fmt.Println("main  ti pos=",ti.Len())
	for {
		tb,err:=ti.GetNoNilTableBytes()
		if err !=nil {
			println("[rebuildkv]  get Table from indexdb err, ", err.Error())
			break
		}
		for key,val := range tb {
			err = Mdb.Rdb.Put(Mdb.wo, key[:], val)
			if err != nil {
				println("[rebuildkv]  batch write to rocksdb err:",err)
				fmt.Println("rbdkv_error")
			}
		}
		atomic.AddUint64(&num,uint64(len(tb)))
	}
	fmt.Println("rbdkv_success")
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
