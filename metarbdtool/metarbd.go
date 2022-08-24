package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"sync/atomic"
	"time"

	"github.com/tecbot/gorocksdb"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/storage"
)

var num uint64
var preNum uint64
var cfg *config.Config
var end chan struct{}
var ti *storage.TableIterator
var preTime time.Time

var Mdb *KvDB

var oldmdbFileName = "/maindb.tmp"
var newmdbFileName = "/maindb"
var ytPosKey    = "yt_rocks_pos_key"
var ytBlkSzKey  = "yt_blk_size_key"

type KvDB struct {
	Rdb *gorocksdb.DB
	ro  *gorocksdb.ReadOptions
	wo  *gorocksdb.WriteOptions
	PosKey ydcommon.IndexTableKey
	PosIdx ydcommon.IndexTableValue
}

func prepareRBD(){
	initRBD()
	cmd1 := exec.Command("/bin/bash","-c","pkill -9 ytfs")
	out,err := cmd1.Output()
	if err != nil{
		if len(out) > 0{
			panic(err)
		}
	}

	fileName := path.Join(util.GetYTFSPath(), "flock.ytfs")
	_,err = os.Create(fileName)
	if err != nil {
		panic(err)
	}
}

func initRBD(){
	var err error
	preTime = time.Now()
	end = make(chan struct{})
	cfg,_=config.ReadConfig()
	pathname := path.Join(util.GetYTFSPath(),"index.db")
	if !PathExists(pathname){
		panic("index.db not exist!")
	}

	ti,err =storage.GetTableIterator(pathname,cfg.Options)
	if err != nil{
		panic(err)
	}
	ti.Reset()
	oldmdbPath := path.Join(util.GetYTFSPath(),oldmdbFileName)
	if PathExists(oldmdbPath){
		err := os.RemoveAll(oldmdbPath)
		if err != nil{
			panic(err)
		}
	}

	mdb,err := openKVDB(oldmdbPath)
	if err != nil {
		log.Println(" open rocksdb err!!")
		panic(err)
	}
	Mdb = mdb
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
	wo.DisableWAL(config.Global_Disable_WAL)

	return &KvDB{
		Rdb   :  db,
		ro    :  ro,
		wo    :  wo,
	}, err
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func setDnIdToKv() error {
	version := ti.GetTableVersion()
	if string(version[:]) == ytfs.OldDbVersion{
		return nil
	}

	dnid := ti.GetDnIdFromTab()
	Bdn := make([]byte,4)
	binary.LittleEndian.PutUint32(Bdn, dnid)
	err := Mdb.Rdb.Put(Mdb.wo, []byte(ytfs.YtfsDnIdKey), Bdn)
	return err
}

func setValue(){
	Pos := ti.Len()
	err := setKVtoDB(Mdb,ytPosKey,uint32(Pos))
	if err != nil {
		log.Println("set start Pos to DB err:",err)
		panic(err)
	}

	BlkSize := ti.BlockSize()
	err = setKVtoDB(Mdb,ytBlkSzKey,BlkSize)
	log.Println("current blksize=",BlkSize)
	if err != nil {
		log.Println("set blocksize to DB err:",err)
		panic(err)
	}

    err = setDnIdToKv()
	if err != nil{
		log.Println("set DnId to kv error:",err)
		panic(err)
	}
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
			log.Printf("tableidx %d copy %d 耗时 %f s进度 %d/%d 百分比 %.4f 预计完成剩余时间%s\n",
				ti.GetTableIndex(),
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

func reNameFile(old,new string){
	oldFilePath := path.Join(util.GetYTFSPath(),old)
	newFilePath := path.Join(util.GetYTFSPath(),new)
	err := os.Rename(oldFilePath,newFilePath)
	if err != nil{
		panic(err)
	}
}

func commitDB(){
	fileName := path.Join(util.GetYTFSPath(), "dbsafe")
	_,err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	cfg.UseKvDb = true
	err = cfg.Save()
	if err != nil{
		panic(err)
	}

	setValue()
	reNameFile("index.db","index.db.bk")

	fileName2 := path.Join(util.GetYTFSPath(), "flock.ytfs")
	err = os.Remove(fileName2)
	if err != nil{
		panic(err)
	}
	reNameFile(oldmdbFileName,newmdbFileName)
}

func main(){
	prepareRBD()
	go printSpeed()
	for {
		tb,err:=ti.GetNoNilTableBytes()
		if err !=nil {
			fmt.Println("[kvdb]GetNoNilTableBytes error:",err.Error())
			if err.Error() != "table_end"{
				log.Println("get Table from indexdb err: ", err,"tableidx=",ti.GetTableIndex())
				panic(err)
			}
			break
		}

		for key,val := range tb {
			err = Mdb.Rdb.Put(Mdb.wo, key[:], val)
			if err != nil {
				log.Println("rbdkv_error:",err,"tableidx=",ti.GetTableIndex())
				panic(err)
			}
		}
		atomic.AddUint64(&num,uint64(len(tb)))
	}
	commitDB()
	log.Println("rbdkv_success")
	end<- struct{}{}
}