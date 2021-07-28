package verifySlice

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
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
	VerifyTms  uint32
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

func SavetoFile( dir, file string,value []byte) error{
	var err error
	status_exist,_ := util.PathExists(dir)
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
	UIbch, err := strconv.ParseUint(resp.VrfBatch,10,8)
	if err != nil {
		return  err
	}
	binary.LittleEndian.PutUint64(Bbch, UIbch)
	_ = vfs.Hdb.DB.Put(vfs.Hdb.Wo, []byte(VerifyBchKey), Bbch)

	if len(resp.ErrShard) <= 0{
		return nil
	}

	for _,v := range resp.ErrShard{
		DBhash := v.DBhash
		_ = vfs.Hdb.DB.Put(vfs.Hdb.Wo, DBhash, Bbch)
	}

	_ = vfs.Bdb.DB.Put(vfs.Bdb.Wo, Bbch, []byte(resp.VrfTime))
	return nil
}

func (vfs *VerifySler)VerifySliceReal(verifyNum string, startItem string) (message.SelfVerifyResp){
	var resp message.SelfVerifyResp

	config, err := config.ReadConfig()
	if err != nil{
		log.Println("[verifyslice] [error] read datanode config error:",err)
		resp.ErrCode = "101"
		return resp
	}
	num,_ := strconv.ParseUint(verifyNum,10,64)

	startItem = compressStr(startItem)

	if config.UseKvDb {
		resp = vfs.VerifySlicekvdb(num, startItem)
		return resp
	}

	resp = vfs.VerifySliceIdxdb(num,startItem)
	return resp
}

func (vfs *VerifySler)VerifySlice(verifyNum string, startItem string) (message.SelfVerifyResp) {
	var resp message.SelfVerifyResp
	var err error

	vfs.Hdb, err = OpenKVDB(Verifyhashdb)
	defer vfs.Hdb.DB.Close()
	if err != nil{
		resp.ErrCode = "ErrOpenHashdb"
		return resp
	}


	vfs.Bdb, err = OpenKVDB(Batchdb)
	defer vfs.Bdb.DB.Close()
	if err != nil{
		resp.ErrCode = "ErrOpenBatchdb"
		return resp
	}

	resp = vfs.VerifySliceReal(verifyNum, startItem)
	if resp.ErrCode == "101"{
		return resp
	}

	//save result to file and verify db
	var UIbtch uint32
	Bbtch, _ := vfs.Hdb.DB.Get(vfs.Hdb.Ro, []byte(VerifyBchKey))
	if ! Bbtch.Exists(){
		UIbtch = 1
	}else{
		UIbtch = binary.LittleEndian.Uint32(Bbtch.Data()) + 1
	}

	Sbtch := strconv.FormatUint(uint64(UIbtch),10)
	Date := Date()
	resp.VrfBatch = Sbtch
	resp.VrfTime = Date
	_ = vfs.SaveVerifyToDb(resp)
	rstdir := util.GetYTFSPath() + Verifyrstdir
	rstfile := "rst"+Date +"_"+ Sbtch
	res, _ := proto.Marshal(&resp)
	_ = SavetoFile(rstdir,rstfile,res)
	return resp
}
