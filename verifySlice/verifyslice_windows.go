package verifySlice

import (
	"github.com/yottachain/YTDataNode/message"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"regexp"
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

func NewVerifySler(sn sni.StorageNode) (*VerifySler){
	return &VerifySler{Sn:sn}
}

func compressStr(str string) string {
	if str == "" {
		return ""
	}

	reg := regexp.MustCompile("\\s+")
	return reg.ReplaceAllString(str, "")
}

func (vfs *VerifySler)MissSliceQuery(key string)(message.SelfVerifyQueryResp){
	var resp message.SelfVerifyQueryResp
	resp.ErrCode="ErrNotSupport"
	//todo: miss slice query not support for windows now
	return resp
}

func (vfs *VerifySler)VerifySlice(verifyNum string, startItem string) (message.SelfVerifyResp){
	var resp message.SelfVerifyResp

	num,_ := strconv.ParseUint(verifyNum,10,64)
	startItem = compressStr(startItem)
	resp = vfs.VerifySliceIdxdb(num, startItem)
	return resp
}
