package slicecompare

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58/base58"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tecbot/gorocksdb"
	"sync"

	//"github.com/tecbot/gorocksdb"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/gc"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	"io/ioutil"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const FinishKey = "key_finishcompareseq"
const Seqkey  = "seqkey_forcompare"
const Comparedb  = "/compare_db/"

const CpStatusDir = "/compare_status/"

var SliceCompareDir string =  CpStatusDir

var Entrycountdownld int32 = 1000

//type CompDB struct {
//	Db  *gorocksdb.DB
//	Ro  *gorocksdb.ReadOptions
//	Wo  *gorocksdb.WriteOptions
//}

//type CompDB sni.CompDB

func init(){
	InitDir(Comparedb)
    InitDir(CpStatusDir)
}

func ForInit(fileName string, value string){
	filePath := util.GetYTFSPath() + fileName
	status_exist,_ := util.PathExists(filePath)
	if status_exist == false {
		content := []byte(value)
		err := ioutil.WriteFile(filePath,content,0666)
		if err != nil{
			fmt.Println(err)
		}
	}
}

func OpenTmpRocksDB(DBName string) (*sni.CompDB, error){
	var tmp sni.CompDB
	DBPath := util.GetYTFSPath() + DBName
	opt := gorocksdb.NewDefaultOptions()
	opt.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opt,DBPath)
	if err != nil{
		fmt.Printf("[slicecompare]open DB:%s error %s",DBPath,err.Error())
		return nil,err
	}

	tmp.Db = db
	tmp.Ro = gorocksdb.NewDefaultReadOptions()
	tmp.Wo = gorocksdb.NewDefaultWriteOptions()

	return &tmp ,nil
}

func GetSeqFromDb(Tdb *sni.CompDB, key string) (uint64, error){
	var seq uint64
	Slc,err := Tdb.Db.Get(Tdb.Ro,[]byte(key))
	if err !=nil || Slc.Data() == nil {
		bval := make([]byte,8)
		binary.LittleEndian.PutUint64(bval,0)
		err = Tdb.Db.Put(Tdb.Wo,[]byte(key),bval)
		return 0, err
	}
	seq = binary.LittleEndian.Uint64(Slc.Data())
	return seq, nil
}

func PutKSeqToDb(seq uint64,hash []byte, Tdb *sni.CompDB) error{
	var err error
	Kseq := make([]byte,8)
	binary.LittleEndian.PutUint64(Kseq,seq)
	err = Tdb.Db.Put(Tdb.Wo, Kseq, hash)
	return err
}

func PutVSeqToDb(seq uint64,hash []byte, Tdb *sni.CompDB) error{
	var err error
	Vseq := make([]byte,8)
	binary.LittleEndian.PutUint64(Vseq,seq)
	err = Tdb.Db.Put(Tdb.Wo, hash, Vseq)
	return err
}

func InitDir(dirName string){
	filePath := util.GetYTFSPath() + dirName
	status_exist,_ := util.PathExists(filePath)
	if status_exist == false {
		err := os.Mkdir(filePath, os.ModePerm)
		if err != nil {
			fmt.Printf("mkdir failed![%v]\n", err)
		} else {
			fmt.Printf("mkdir success!\n")
		}
	}
}

//type SliceCompareHandler func(dataFromSn *message.ListDNIResp) bool

type SliceComparer struct {
	Sn sni.StorageNode
	Lock  sync.Mutex
}

func (sc *SliceComparer) ChkStatusFile(msg message.SliceCompareReq) error{
	var status message.SliceCompareStatusResp
	var err error

	filePath := util.GetYTFSPath() + CpStatusDir + msg.TaskId
	//status_exist,_ := util.PathExists(filePath)
	//if status_exist {
	//	value,_ := ioutil.ReadFile(filePath)
	//	_ = proto.Unmarshal(value,&status)
	//	if status.ErrCode != "Running" {
	//		err = fmt.Errorf("TaskRunned")
	//		return err
	//	}
	//}

	status.ErrCode = "Running"
	stus,_ := proto.Marshal(&status)
	err = SavetoFile(filePath,stus)
	if err != nil{
		fmt.Println("Save start compare status to file, error:",err)
		return err
	}

	return nil
}

func (sc *SliceComparer)CompareMsgChkHdl(data []byte) (message.SliceCompareResp, error) {
	var msg message.SliceCompareReq
	var res message.SliceCompareResp
	var err error

	//sc.Lock.Lock()

	res.NodeId = sc.Sn.Config().IndexID
	if err := proto.Unmarshal(data, &msg); err != nil {
		log.Println("[gcdel] message.slicecomparereq error:", err)
		res.ErrCode = "errReq"
		res.TaskId = msg.TaskId
		err := fmt.Errorf("errReq")
		return res, err
	}

	res.TaskId = msg.TaskId
	if err = sc.ChkStatusFile(msg); err != nil{
		res.ErrCode = err.Error()
		return res, err
	}

	res.TaskId = msg.TaskId
	if msg.NodeId != sc.Sn.Config().IndexID{
		log.Println("[gcdel] message.NodeId error")
		res.ErrCode = "errNodeid"
		err := fmt.Errorf("errNodeid")
		return res, err
	}

	Tdb := sc.Sn.GetCompareDb()

	go sc.RunRealCompare(msg, Tdb)
	res.ErrCode = "Succ"
	//sc.Lock.Unlock()

	return res, err
}

func SavetoFile(filePath string,value []byte) error{
	var status message.SliceCompareStatusResp
	var err error
	status_exist,_ := util.PathExists(filePath)
	if status_exist {
		value,_ := ioutil.ReadFile(filePath)
		_ = proto.Unmarshal(value,&status)
		if status.ErrCode != "Running" {
			err = fmt.Errorf("TaskRunned")
			return err
		}
	}

	err = ioutil.WriteFile(filePath, value,0666)
	if err != nil{
		fmt.Println("[gcdel] save value to file error",err,"filepath:",filePath)
		err = fmt.Errorf("SaveFileErr")
		return err
	}
	return nil
}

func (sc *SliceComparer) RunRealCompare(msg message.SliceCompareReq, Tdb *sni.CompDB){
	//return
	filePath := util.GetYTFSPath() + CpStatusDir + msg.TaskId
	res,_ := sc.CompareHashFromSn(msg, Tdb)
	stus,_ := proto.Marshal(&res)
	_ = SavetoFile(filePath,stus)
}

func (sc *SliceComparer)GetCompareStatus(msg message.SliceCompareStatusReq) (message.SliceCompareStatusResp){
	var res message.SliceCompareStatusResp
	res.ErrCode = "succ"
	res.TaskId = msg.TaskId
	filePath := util.GetYTFSPath() + CpStatusDir + msg.TaskId
	log.Println("[gcdel] getGcStatus, taskid:",msg.TaskId)

	status_exist,_ := util.PathExists(filePath)
	if ! status_exist {
		fmt.Println("[gcdel] statusfile not exist,filepath:",filePath)
		res.ErrCode = "Nofile"
		return res
	}

	value, err := ioutil.ReadFile(filePath)
	if err != nil{
		fmt.Println("[gcdel] read status file error:",err,"filepath:",filePath)
		res.ErrCode = "fileRdErr"
		return res
	}

	err = proto.Unmarshal(value,&res)
	if err !=nil{
		fmt.Println("[gcdel] unmarshal statusfile to resp error:",err,"filepath:",filePath)
		res.ErrCode = "fileUnmarshalErr"
	}
	return res
}

func (sc *SliceComparer)CompareMsgStatusChkHdl(data []byte)(message.SliceCompareStatusResp, error){
	var res message.SliceCompareStatusResp
	var msg message.SliceCompareStatusReq
	var err error

	res.NodeId = sc.Sn.Config().IndexID

	if err := proto.Unmarshal(data, &msg); err != nil {
		log.Println("[gcdel] message.GcReq error:", err)
		res.ErrCode = "errstatusreq"
		return res, err
	}

	res.TaskId = msg.TaskId

	if !config.Gconfig.SliceCompareOpen{
		//res.ErrCode = "errNotOpenGc"
		//return res, err
	}

	if msg.NodeId != sc.Sn.Config().IndexID{
		log.Println("[gcdel] message.GcReq error:", err)
		res.ErrCode = "errNodeid"
		return res, err
	}

	res = sc.GetCompareStatus(msg)

	return res, err
}

func (sc *SliceComparer) RedundencySliceGc(msg message.SliceCompareReq,resaddr *message.SliceCompareStatusResp,Tdb *sni.CompDB){
	GcW := &gc.GcWorker{sc.Sn}
    BStartSeq := make([]byte,8)
    binary.LittleEndian.PutUint64(BStartSeq,msg.StartSeq)
	snmissseq := make([]uint64, 1)

    if !sc.Sn.Config().UseKvDb{
	    (*resaddr).ErrCode = "ErrDbForGc"
    	log.Println("[slicecompare][error] use indexdb, gc failed")
	    return
    }

	comparedseq, err := GetSeqFromDb(Tdb, FinishKey)
	if err != nil{
		(*resaddr).ErrCode = "ErrGetComparedSeq"
		fmt.Println("[slicecompare] error:", (*resaddr).ErrCode)
	}

	Tdb.Ro.SetFillCache(false)
    iter := Tdb.Db.NewIterator(Tdb.Ro)
    for iter.SeekToFirst(); iter.Valid(); iter.Next(){
    	Bkey := iter.Key().Data()
    	if Bkey == nil || len(Bkey) > 8 {
    		continue
	    }

	    seq := binary.LittleEndian.Uint64(Bkey)
	    if seq < msg.StartSeq || seq < comparedseq {
	    	_ = Tdb.Db.Delete(Tdb.Wo, Bkey)
	    	continue
	    }

	    if seq > msg.EndSeq {
	    	break
	    }

	    GcHash := iter.Value().Data()
	    if len(GcHash) != 16 {
	    	continue
	    }

	    err := GcW.GcHashProcess(GcHash)
	    if err != nil{
	    	fmt.Println("[slicecompare] gc error:", err, "hash",base58.Encode(GcHash))
	    }
	    _ = Tdb.Db.Delete(Tdb.Wo, Bkey)
	    snmissseq = append(snmissseq, seq)
	    //(*resaddr).DnMissList = append((*resaddr).DnMissList, GcHash)
	    (*resaddr).SnMissNum++
    }

	err = PutVSeqToDb(msg.EndSeq, []byte(FinishKey), Tdb)
	if err != nil{
		log.Println("[slicecompare] ErrPutComparedSeq")
	}

	if len(snmissseq) > 0{
		log.Println("[slicecompare]task:",msg.TaskId," snmissseq:",snmissseq)
	}
}

func (sc *SliceComparer)CompareHashFromSn(msg message.SliceCompareReq, Tdb *sni.CompDB) (message.SliceCompareStatusResp, error){
	var seq uint64
	var hash string
	var res message.SliceCompareStatusResp

	res.NodeId = msg.NodeId
	res.TaskId = msg.TaskId
	res.ErrCode = "succ"
	res.StartSeq = msg.StartSeq
	res.EndSeq = msg.EndSeq
	BKey := make([]byte,8)
	comparedseq, err := GetSeqFromDb(Tdb, FinishKey)
	if err != nil{
		res.ErrCode = "ErrGetComparedSeq"
		fmt.Println("[slicecompare] error:", res.ErrCode)
		return res, err
	}

	//res.CompareNum = uint32(msg.EndSeq - msg.StartSeq)
	for _, seqtohash := range msg.CpList {
		atomic.AddUint32(&res.CompareNum,1)
		seq = seqtohash.Seq
		if seq < comparedseq || seq < msg.StartSeq || seq > msg.EndSeq {
			continue
		}


		binary.LittleEndian.PutUint64(BKey, seq)
		dnhash,err := Tdb.Db.Get(Tdb.Ro, BKey)

		if err != nil || len(dnhash.Data()) != 16{
           fmt.Printf("[slicecompare] get hash of seq %v, hash %v, error %v \n",seq, seqtohash.Hash, err)
           res.DnMissList = append(res.DnMissList, seqtohash.Hash)
           atomic.AddUint32(&res.DnMissNum,1)
           continue
		}

		if len(seqtohash.Hash) != 16 {
			continue
		}

		hash = base58.Encode(seqtohash.Hash)
		Strdnhash := base58.Encode(dnhash.Data())
		if Strdnhash != hash{
			fmt.Println("[slicecompare] error, dnhash:",Strdnhash,"snhash:",hash)
			res.DnMissList = append(res.DnMissList, seqtohash.Hash)
			atomic.AddUint32(&res.DnMissNum,1)
			continue
		}

		err = Tdb.Db.Delete(Tdb.Wo, BKey)
		if err != nil{
			fmt.Println("[slicecompare] delete item from compare_db error, dnhash:",Strdnhash,"snhash:",hash)
			res.DnMissList = append(res.DnMissList, seqtohash.Hash)
			atomic.AddUint32(&res.DnMissNum,1)
			continue
		}
	}

	sc.RedundencySliceGc(msg, &res, Tdb)
	return res, err
}

func (sc *SliceComparer)CompareDelStatusfile(msg message.CpDelStatusfileReq) (message.CpDelStatusfileResp){
	var res message.CpDelStatusfileResp
	res.TaskId = msg.TaskId
	res.NodeId = msg.NodeId
	res.ErrCode = "Succ"

	filePath := util.GetYTFSPath() + CpStatusDir + msg.TaskId
	log.Println("[gcdel] getGcStatus, taskid:",msg.TaskId)

	status_exist,_ := util.PathExists(filePath)
	if ! status_exist {
		fmt.Println("[gcdel] statusfile not exist,filepath:",filePath)
		res.ErrCode = "NoFile"
		return res
	}

	err := os.Remove(filePath)
	if err !=nil {
		fmt.Println("[gcdel] delete status file error:",err)
		res.ErrCode= "DelErr"
	}
	return res
}

func (sc *SliceComparer)CompareMsgDelfileHdl(data []byte)(message.CpDelStatusfileResp, error){
	var msg message.CpDelStatusfileReq
	var res message.CpDelStatusfileResp
	var err error

	res.NodeId = sc.Sn.Config().IndexID

	if err := proto.Unmarshal(data, &msg); err != nil {
		log.Println("[gcdel] message.GcReq error:", err)
		res.ErrCode = "errdelreq"
		return res, err
	}

	res.TaskId = msg.TaskId
	if !config.Gconfig.SliceCompareOpen{
		//res.ErrCode = "errNotOpenGc"
		//err = fmt.Errorf("errNotOpenGc")
		//return res, err
	}

	if msg.NodeId != sc.Sn.Config().IndexID{
		log.Println("[gcdel] message.GcReq error:", err)
		res.ErrCode = "errNodeid"
		err = fmt.Errorf("errNodeid")
		return res, err
	}

	res = sc.CompareDelStatusfile(msg)
	return res, nil
}

//func (sc *SliceComparer)SaveRecordToTmpDB(hashBatch [][]byte, db *leveldb.DB) error {
//	return nil                       //TODO  close slicecompare
//	var err error
//	nowtime := strconv.FormatInt(time.Now().Unix(),10)
//    for _, key := range hashBatch{
//		nowtime = strconv.FormatInt(time.Now().Unix(),10)
//		err = db.Put(key, []byte(nowtime), nil)
//		if err !=nil {
//			fmt.Println("[slicecompare][error]put dnhash to temp_index_kvdb error",err)
//			return err
//		}
//	}
//	return err
//}

//func (sc *SliceComparer)SaveSnRecordToDB(hashBatch [][]byte, fileSnDBName string) error {
//	var strval string
//	DBPath := util.GetYTFSPath() + fileSnDBName
//	db,err := leveldb.OpenFile(DBPath,nil)
//	if err != nil{
//		fmt.Printf("open tmpDB:%s error",DBPath)
//		return err
//	}
//	defer db.Close()
//
//	for value, key := range hashBatch{
//		strval = strconv.Itoa(value)
//		err = db.Put(key, []byte(strval), nil)
//		if err !=nil {
//			fmt.Println("[slicecompare][error]put snhash to sn_index_kvdb error",err)
//			return err
//		}
//	}
//	return err
//}

func (sc *SliceComparer)GetAllReordFromDB(fileName string){
	DBPath := util.GetYTFSPath() + fileName
	log.Println(DBPath)
	db,err := leveldb.OpenFile(DBPath,nil)
	if err != nil{
		fmt.Printf("open tmpDB:%s error",DBPath)
		return
	}
	defer db.Close()
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		value := string(iter.Value())
		fmt.Printf("key[%s]=[%s]\n",base58.Encode(iter.Key()),value);
	}
}

func GetValueFromFile(fileName string) (string ,error){
	filePath := util.GetYTFSPath() + fileName
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("read file err=%v\r\n", err)
	}
	return string(content),err
}

func (sc *SliceComparer)SaveEntryInDBToDel(tmp_db *leveldb.DB, toDelEntryDB string, comparetimes uint8) error {
	var saveTime string
	var saveTimeInt int64

	if comparetimes < 3{
		return nil
	}

	nowTime := time.Now().Unix()
	toDelDBPath := util.GetYTFSPath() + toDelEntryDB
	del_db,err := leveldb.OpenFile(toDelDBPath,nil)
	if err != nil{
		log.Println(err)
		return err
	}
	defer del_db.Close()

	iter := tmp_db.NewIterator(nil, nil)
	for iter.Next() {
		saveTime = string(iter.Value())
		saveTimeInt,_ = strconv.ParseInt(saveTime,10,64)

		if nowTime - saveTimeInt >= 1200 {
			key := iter.Key()
			value := iter.Value()
           if err := del_db.Put(key,value,nil); err != nil{
           	   log.Println(err)
			   return err
		   }
		   log.Printf("[slicecompare][dn_slice_error] key=%s saved in datanode, but not found in supernode!!",base58.Encode(key))
           if err := tmp_db.Delete(key,nil); err != nil{
           	   log.Println(err)
           	   return err
		   }
		}
	}
	return err
}

func SaveValueToFile(Value string, FileName string) error {
    filePath := util.GetYTFSPath() + FileName
	err := ioutil.WriteFile(filePath,[]byte(Value),0666)
	if err != nil{
		fmt.Println(err)
	}
	return err
}

func cleanDB(nameOfDB string){
	needClearDBPath := util.GetYTFSPath() + nameOfDB
	cle_db,_ := leveldb.OpenFile(needClearDBPath ,nil)
	defer cle_db.Close()

	iter := cle_db.NewIterator(nil, nil)
	for iter.Next() {
		if err := cle_db.Delete(iter.Key(),nil); err != nil{
				log.Println(err)
				return
		}
	}
}

