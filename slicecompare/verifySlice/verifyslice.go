package verifySlice

import (
	"github.com/yottachain/YTDataNode/message"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
	"strconv"
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

func (vfs *VerifySler)VerifySlice(verifyNum string) (message.SelfVerifyResp){
	var resp message.SelfVerifyResp

	config, err := config.ReadConfig()
	if err != nil{
		log.Println("[verifyslice] [error] read datanode config error:",err)
		resp.ErrCode = "101"
		return resp
	}

	num,err := strconv.ParseUint(verifyNum,10,64)

	if config.UseKvDb {
		resp = vfs.VerifySlicekvdb(num)
		return resp
	}

	resp = vfs.VerifySliceIdxdb(num)
	return resp
}
