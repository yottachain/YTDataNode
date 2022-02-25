package verifySlice

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/tecbot/gorocksdb"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"

	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"time"
)

/* verify errCode
    000   success
    100   verify message request err
	101   open datanode config file(config.json) error
    102   open indexdb file error
	103   read header of indexdb error
    200   verify slice function error
*/

type VrDB struct {
	DB *gorocksdb.DB
	Ro  *gorocksdb.ReadOptions
	Wo  *gorocksdb.WriteOptions
}

type VerifySler struct {
	Sn         sni.StorageNode
	Hdb        *VrDB
	Bdb        *VrDB
}

func NewVerifySler(sn sni.StorageNode) (*VerifySler){
	Hdb, err := OpenKVDB(Verifyhashdb)
	if err != nil {
		fmt.Println("[verify] Open hashdb error:",err)
		return nil
	}

	Bdb, err := OpenKVDB(Batchdb)
	if err != nil{
		fmt.Println("[verify] Open Batchdb error:",err)
		return nil
	}

	return &VerifySler{sn,Hdb,Bdb,}
}

func compressStr(str string) string {
	if str == "" {
		return ""
	}

	reg := regexp.MustCompile("\\s+")
	return reg.ReplaceAllString(str, "")
}

func Date() string{
	NowTime := time.Now()
	//NowHour := time.Now().Hour()
	year := strconv.Itoa(NowTime.Year())
	month := strconv.Itoa(int(NowTime.Month()))
	day := strconv.Itoa(NowTime.Day())
	date := year+"-"+month+"-"+day
	return date
}

func SavetoFile(dir, file string, value []byte) error{
	var err error
	status_exist, _ := util.PathExists(dir)
	if !status_exist {
		os.MkdirAll(dir, 0666)
	}

	filePath := dir + file
	err = ioutil.WriteFile(filePath, value,0666)
	if err != nil{
		fmt.Println("[gcdel] save value to file error",err,"filepath:",filePath)
		err = fmt.Errorf("SaveFileErr")
		return err
	}
	return nil
}

func (vfs *VerifySler)SaveVerifyToDb(resp message.SelfVerifyResp) error {
	Bbch := make([]byte, 8)
	UIbch, err := strconv.ParseUint(resp.VrfBatch,10,64)
	if err != nil {
		return  err
	}
	binary.LittleEndian.PutUint64(Bbch, UIbch)
	err = vfs.Hdb.DB.Put(vfs.Hdb.Wo, []byte(VerifyBchKey), Bbch)
	if err != nil {
		log.Printf("verify hdb put VerifyBchKey fail butch is %d\n", UIbch)
	}

	if len(resp.ErrShard) <= 0 {
		return nil
	}

	for _,v := range resp.ErrShard {
		DBhash := v.DBhash
		log.Printf("verify hdb put shard:%s\n", base58.Encode(DBhash))
		err = vfs.Hdb.DB.Put(vfs.Hdb.Wo, DBhash, Bbch)
		if err != nil {
			log.Printf("verify hdb put fail shard:%s\n", base58.Encode(DBhash))
		}
	}

	_ = vfs.Bdb.DB.Put(vfs.Bdb.Wo, Bbch, []byte(resp.VrfTime))
	return nil
}

func (vfs *VerifySler)VerifySliceReal(verifyNum uint32, startItem string) (message.SelfVerifyResp){
	var resp message.SelfVerifyResp

	config, err := config.ReadConfig()
	if err != nil{
		log.Println("[verifyslice] [error] read datanode config error:",err)
		resp.ErrCode = "101"
		return resp
	}

	startItem = compressStr(startItem)

	if config.UseKvDb {
		resp = vfs.VerifySlicekvdb(verifyNum, startItem)
		return resp
	}

	resp = vfs.VerifySliceIdxdb(verifyNum, startItem)
	return resp
}

func (vfs *VerifySler)MissSliceQuery(Skey string)(message.SelfVerifyQueryResp){
	var resp message.SelfVerifyQueryResp
	resp.Key = Skey
	resp.ErrCode = "Succ"
	HKey, err := base58.Decode(Skey)
	if err != nil {
		resp.ErrCode = "ErrDecodeKey"
		fmt.Println("Decode key error:",err)
		return resp
	}

	VrfBch, err := vfs.Hdb.DB.Get(vfs.Hdb.Ro, HKey)
	if err != nil {
		resp.ErrCode = "ErrGetBatchNum"
		fmt.Println("Get BatchNum of ", Skey," error",err.Error())
		return resp
	}

	if !VrfBch.Exists(){
		resp.ErrCode = "ErrNoHashKey"
		fmt.Println("error, hash not exist, key:", Skey, "vrfbch", VrfBch)
		return resp
	}
	Bbch := VrfBch.Data()
	UIBch := binary.LittleEndian.Uint64(Bbch)
	resp.BatchNum = uint32(UIBch)

	VrfTm, err := vfs.Bdb.DB.Get(vfs.Bdb.Ro, Bbch)
	if err != nil {
		resp.ErrCode = "ErrGetBatchTm"
		fmt.Println("Get Verify-time of ", UIBch, " error", err.Error())
		return resp
	}

	if !VrfTm.Exists(){
		resp.ErrCode = "ErrNoBatchTm"
		fmt.Println("error, batchnum not exist, batchnum:",UIBch)
		return resp
	}

	STime := string(VrfTm.Data())
	resp.Date = STime
	fmt.Println("[query result] Hash:", Skey, "BatchNum:", UIBch, "Time:", STime)

	return resp
}

//func (vfs *VerifySler)VerifySlice(verifyNum string, startItem string) (*message.SelfVerifyResp) {
func (vfs *VerifySler)VerifySlice(verifyNum uint32, startItem string) (*message.SelfVerifyResp) {
	resp := new(message.SelfVerifyResp)
	var err error

	rstdir := util.GetYTFSPath() + Verifyrstdir
	err = ChkRsvSpace(rstdir, 1048576)
	if err != nil{
		log.Println("[verify] ChkRsvSpace error:",err.Error())
		resp.ErrCode = "ErrChkRsvSpace"
		return resp
	}

	if nil == vfs.Hdb {
		resp.ErrCode = "ErrOpenHashdb"
		return resp
	}

	if nil == vfs.Bdb {
		resp.ErrCode = "ErrOpenBatchdb"
		return resp
	}

	log.Printf("[verify] VerifySlice verify num %d, start key %s\n", verifyNum, startItem)
	*resp = vfs.VerifySliceReal(verifyNum, startItem)
	if resp.ErrCode != "000" {
		return resp
	}

	//save result to file and verify db
	var UIbtch uint32
	Bbtch, _ := vfs.Hdb.DB.Get(vfs.Hdb.Ro, []byte(VerifyBchKey))
	if ! Bbtch.Exists() {
		UIbtch = 1
	}else{
		UIbtch = binary.LittleEndian.Uint32(Bbtch.Data()) + 1
	}

	Sbtch := strconv.FormatUint(uint64(UIbtch),10)
	Date := Date()
	resp.VrfBatch = Sbtch
	resp.VrfTime = Date
	resp.Num = strconv.FormatUint(uint64(verifyNum), 10)
	_ = vfs.SaveVerifyToDb(*resp)
	rstfile := "rst" + Date + "_"+ Sbtch
	res, _ := proto.Marshal(resp)
	_ = SavetoFile(rstdir, rstfile, res)
	return resp
}

func (vfs *VerifySler) TravelHDB(fn func(key, value []byte) error) int64{
	iter := vfs.Hdb.DB.NewIterator(vfs.Hdb.Ro)
	succ := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.Key().Size() != 16 {
			continue
		}
		if err := fn(iter.Key().Data(), iter.Value().Data()); err != nil {
			fmt.Println("[travelDB] exec fn() err=", err, "key=", iter.Key().Data(), "value=", iter.Value().Data())
			continue
		}
		succ++
	}
	return int64(succ)
}