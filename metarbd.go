package main

import (
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"github.com/tecbot/gorocksdb"

	"github.com/yottachain/YTFS/storage"
	"log"
	"sync/atomic"
	"time"
	"path"
	"github.com/yottachain/YTDataNode/util"
//	"github.com/yottachain/YT"
)

var num uint64
var preNum uint64
var cfg *config.Config
var end chan struct{}
var ti *storage.TableIterator
var preTime time.Time
var glbti storage.TableIterator

type KvDB struct{
	Rdb *gorocksdb.DB
	ro  *gorocksdb.ReadOptions
	wo  *gorocksdb.WriteOptions
}

var Mdb *KvDB

func openKVDB(DBPath string) (kvdb *KvDB,err error){
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
		return nil,err
	}

	// 创建输入输出流
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	return &KvDB {
		Rdb: db,
		ro:  ro,
		wo:  wo,
	},err
}

func init(){
//	var glbti storage.TableIterator
	preTime = time.Now()
	end = make(chan struct{})
	cfg,_=config.ReadConfig()
	pathname1 := path.Join(util.GetYTFSPath(),"index.db")
	pathname2 := path.Join(util.GetYTFSPath(),"metadata.db")
	fmt.Println("pathname2=",pathname2)
	ti,_=storage.GetTableIterator2(pathname1,pathname2,cfg.Options,glbti)
	fmt.Println("init  ti=",ti)
//	db,err := OpenFile(path.Join(util.GetYTFSPath(),"maindb"),nil)
	mdb,err := openKVDB(path.Join(util.GetYTFSPath(),"maindb"))
	if err != nil {
		println("[rebuildkv] open rocksdb err!!")
		panic(err)
	}
    Mdb = mdb
}

func main(){
//	init_env()
	go printSpeed()
	for {
		fmt.Println("main  ti=",ti)
//		fmt.Println("ti",ti.)
		tb,err:=ti.GetNoNilTableBytes()
		if err !=nil {
			println("[rebuildkv]  get Table from indexdb err, ", err.Error())
			break
		}
//		Wbatch := new(gorocksdb.WriteBatch)
		for key,val := range tb {
			err = Mdb.Rdb.Put(Mdb.wo, key[:], val)
			if err != nil {
				println("[rebuildkv]  batch write to rocksdb err!!")
				log.Println(err)
				fmt.Println("rbdkv_error")
			}
		}
//		err=Mdb.Rdb.Write(Mdb.wo,Wbatch)
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
