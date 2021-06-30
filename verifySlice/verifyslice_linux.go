package verifySlice

import (
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"regexp"
)

/* verify errCode
    000   success
    100   verify message request err
	101   open datanode config file(config.json) error
    102   open indexdb file error
	103   read header of indexdb error
    200   verify slice function error
*/

type VerifySler struct {
	Sn sni.StorageNode
}


func compressStr(str string) string {
	if str == "" {
		return ""
	}

	reg := regexp.MustCompile("\\s+")
	return reg.ReplaceAllString(str, "")
}

func (vfs *VerifySler)VerifySlice(verifyNum string, startItem string) (message.SelfVerifyResp){
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
