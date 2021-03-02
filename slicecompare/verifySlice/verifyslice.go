package verifySlice

import (
	"github.com/yottachain/YTDataNode/message"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
)

/* verify errCode
    000   success
	101   open datanode config file(config.json) error
    102   open indexdb file error
	103   read header of indexdb error



*/



type VerifySler struct {
	sni.StorageNode
}

func (vfs *VerifySler)VerifySlice() (message.SelfVerifyResp){
	var resp message.SelfVerifyResp

	config, err := config.ReadConfig()
	if err != nil{
		log.Println("[verifyslice] [error] read datanode config error:",err)
		resp.ErrCode = "101"
		return resp
	}

	if config.UseKvDb {
		resp = vfs.VerifySlicekvdb()
		return resp
	}

	resp = vfs.VerifySliceIdxdb()
	return resp
}
