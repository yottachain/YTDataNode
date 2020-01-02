package slicecompare

import (
	"fmt"
	"github.com/mr-tron/base58/base58"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/yottachain/YTCrypto/common"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/util"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

var SliceCompareDir string = "/" + "gc"
var FileFirstEntryIdx string ="/" + "gc/first_entry_index_file"
var ComparedIdxFile string = "/" + "gc/last_compared_index_file"

var FileDB_tmp string = "/" + "gc/temp_index_kvdb"
var FileDB_sn string = "/" + "gc/sn_index_kvdb"
var FileDB_todel string = "/" + "gc/entry_to_del_kvdb"
var Entrycountdownld int32 = 1000


func init(){
    initDir(SliceCompareDir)
    forInit(FileFirstEntryIdx,"000000000000000000000001")
	forInit(ComparedIdxFile,"000000000000000000000000")
}

func forInit(fileName string, value string){
	filePath := util.GetYTFSPath() + fileName
	status_exist := common.FileExist(filePath)
	if status_exist == false {
		content := []byte(value)
		err := ioutil.WriteFile(filePath,content,0666)
		if err != nil{
			fmt.Println(err)
		}
	}
}

func initDir(dirName string){
	filePath := util.GetYTFSPath() + dirName
	status_exist := common.FileExist(filePath)
	if status_exist == false {
		err := os.Mkdir(filePath, os.ModePerm)
		if err != nil {
			fmt.Printf("mkdir failed![%v]\n", err)
		} else {
			fmt.Printf("mkdir success!\n")
		}
	}
}

type SliceCompareHandler func(dataFromSn *message.ListDNIResp) bool

type SliceComparer struct {
	SliceCompareDir     string
	File_TmpDB          string
	File_SnDB           string
	File_ToDelDB        string
	ComparedIdxFile     string
	FirstEntryIdxFile   string
	Entrycountdownld    int32
	CompareTimes        uint8
}

func NewSliceComparer() *SliceComparer {
	return &SliceComparer{
		SliceCompareDir : SliceCompareDir,
		File_TmpDB : FileDB_tmp,
		File_SnDB : FileDB_sn,
		File_ToDelDB : FileDB_todel,
		ComparedIdxFile : ComparedIdxFile,
		FirstEntryIdxFile : FileFirstEntryIdx,
		Entrycountdownld : Entrycountdownld,
		CompareTimes : 0,
	}
}


func (sc *SliceComparer)SaveRecordToTmpDB(hashBatch [][]byte, tmpDBName string) error {
	DBPath := util.GetYTFSPath() + tmpDBName
	db,err := leveldb.OpenFile(DBPath,nil)
	defer db.Close()
	nowtime := strconv.FormatInt(time.Now().Unix(),10)
    for _, key := range hashBatch{
		nowtime = strconv.FormatInt(time.Now().Unix(),10)
		err = db.Put(key, []byte(nowtime), nil)
		if err !=nil {
			fmt.Println(err)
			return err
		}
	}
	return err
}

func (sc *SliceComparer)SaveSnRecordToDB(hashBatch [][]byte, fileSnDBName string) error {
	var strval string
	DBPath := util.GetYTFSPath() + fileSnDBName
	db,err := leveldb.OpenFile(DBPath,nil)
	defer db.Close()

//	sc.saveFirstEntryNumToFile(firstEntryIdx,firstEntryIdxFile)

	for value, key := range hashBatch{
		strval = strconv.Itoa(value)
		err = db.Put(key, []byte(strval), nil)
		if err !=nil {
			fmt.Println(err)
			return err
		}
	}
	return err
}

func (sc *SliceComparer)GetAllReordFromDB(fileName string){
	DBPath := util.GetYTFSPath() + fileName
	log.Println(DBPath)
	db,_ := leveldb.OpenFile(DBPath,nil)
	defer db.Close()
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		value := string(iter.Value())
		fmt.Printf("key[%s]=[%s]\n",base58.Encode(iter.Key()),value);
	}
}

func (sc *SliceComparer)GetValueFromFile(fileName string) (string ,error){
	filePath := util.GetYTFSPath() + fileName
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("read file err=%v\r\n", err)
	}
	return string(content),err
}

func (sc *SliceComparer)CompareEntryWithSnTables(snHashBatch [][]byte, tmpHashDB, snHashDB, comparedIdxFileName, nextID string, comparetimes * uint8) error {

	tmpDBPath := util.GetYTFSPath() + tmpHashDB
	tmp_db,_ := leveldb.OpenFile(tmpDBPath,nil)
	defer tmp_db.Close()

	snDBPath := util.GetYTFSPath() + snHashDB
	sn_db,_ := leveldb.OpenFile(snDBPath,nil)
	defer sn_db.Close()

	filePath := util.GetYTFSPath() + comparedIdxFileName
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, 0666)
    if err != nil{
    	fmt.Printf("open file:%s error",comparedIdxFileName)
    	return err
	}

	err = ioutil.WriteFile(filePath,[]byte(nextID),0666)
	if err != nil{
		fmt.Println(err)
	}
	defer f.Close()

	nowtime := strconv.FormatInt(time.Now().Unix(),10)

	for _,key := range snHashBatch {
		err = sn_db.Put(key, []byte(nowtime), nil)
		if ok,err := tmp_db.Has(key,nil); ok == true{
            err = tmp_db.Delete(key,nil)
            if err != nil{
				log.Println("[error]delete item from tmp_db error",err)
            	return err
			}
		}else{
			log.Printf("[Fatal error] key=%s saved in supernode, but not found in datanode!!",base58.Encode(key))
			return err
		}
	}
	*comparetimes++
	return err
}

func (sc *SliceComparer)SaveEntryInDBToDel(tmpHashDB, toDelEntryDB string, comparetimes uint8) error {
	var saveTime string
	var saveTimeInt int64

	if comparetimes < 3{
		return nil
	}

	nowTime := time.Now().Unix()
    tmpHashDBPath := util.GetYTFSPath() + tmpHashDB
	tmp_db, err := leveldb.OpenFile(tmpHashDBPath,nil)
	if err != nil{
	   log.Println(err)
	   return err
	}

	defer tmp_db.Close()

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
		if nowTime - saveTimeInt >= 600 {
           if err := del_db.Put(iter.Key(),iter.Value(),nil); err != nil{
           	   log.Println(err)
           	   return err
		   }
           if err := tmp_db.Delete(iter.Key(),nil); err != nil{
           	   log.Println(err)
           	   return err
		   }
		}
	}
	return err
}

func (sc *SliceComparer)saveFirstEntryNumToFile(firstEntryIdx string, firstEntryIdxFile string) error {
    filePath := util.GetYTFSPath() + firstEntryIdxFile
	err := ioutil.WriteFile(filePath,[]byte(firstEntryIdx),0666)
	if err != nil{
		fmt.Println(err)
	}
	return err
}

func (sc *SliceComparer)cleanDB(nameOfDB string){
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