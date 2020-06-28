package confirmSlice

import (
	"bytes"
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/slicecompare"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	ydcommon "github.com/yottachain/YTFS/common"
//	"github.com/yottachain/YTDataNode/config"
	"io"
	"os"
	"path"
	"strconv"
	"unsafe"
)

var VarifyedNumFile string = "/gc/n_file"
var hash1Str = "1111111111111111"
var hash0Str = "0000000000000000"

type ConfirmSler struct {
	sni.StorageNode
}

func init(){
	slicecompare.InitDir(slicecompare.SliceCompareDir)
	slicecompare.ForInit(VarifyedNumFile,"0")
}

func (cfs *ConfirmSler)SliceHashVarify(n, m, h, start_Item uint64, fl_IdxDB *os.File) (uint64,uint64) {
	var i uint64
	var errCount uint64
	var indexKey [16]byte
	var varifyedItem = start_Item
	n_Rangeth := start_Item/m                 //range zoom index
	m_Itermth := start_Item%m
	buf := make([]byte,16,16)
	begin := true

	for {
		log.Printf("[confirmslice] verify_parameter: n=%v,m=%v,n_Rangeth=%v", n, m, n_Rangeth)
		if n_Rangeth > n {
			log.Println("[confirmslice] all hash in indexdb has verified, will to return!")
			slicecompare.SaveValueToFile(strconv.FormatUint(0, 10), VarifyedNumFile)
			goto OUT
		}

		pos := n_Rangeth*(4+m*20) + h + 4
		for i = 0; i < m; i++ {
			if begin {
				i = m_Itermth
				pos = pos + 20*i
				begin = false
			}
			fl_IdxDB.Seek(int64(pos), io.SeekStart)
			k, err := fl_IdxDB.Read(buf)
			if (err != nil) || (k != 16) {
				log.Printf("[confirmslice] [error] read hash from index.db to buf,k=%d", k)
				continue
			}

			copy(indexKey[:], buf[0:16])
			pos = pos + 20

			if base58.Encode(indexKey[:]) == hash0Str || base58.Encode(indexKey[:]) == hash1Str {
				if i % 1000 == 0 {
					log.Printf("[confirmslice] the hash not valide,n=%v,m=%v,n_Rangeth=%v,VHF=%v",n,m,n_Rangeth,base58.Encode(indexKey[:]))
				}
				//continue
			}

			resData, err := cfs.YTFS().Get(indexKey)
			if err != nil {
				log.Println("[confirmslice] error:", err, " VHF:", base58.Encode(indexKey[:]))
				//continue
			}

			if ! message.VerifyVHF(resData, indexKey[:]) {
					if i % 1000 == 0 {
						log.Printf("[confirmslice][hashdataok] n=%d,m=%d,n_Rangeth=%d,VHF=%s", n, m, n_Rangeth, base58.Encode(indexKey[:]))
					}
				}else{
				log.Println("[confirmslice]verify failed,VHF:", base58.Encode(indexKey[:]))
				errCount++
			}

			varifyedItem++
			if varifyedItem >= start_Item+1000 {
				log.Println("[confirmslice] Has verified 1000 item, will to return!")
				slicecompare.SaveValueToFile(strconv.FormatUint(varifyedItem, 10), VarifyedNumFile)
				goto OUT
			}
		}
		n_Rangeth++
	}
OUT:
	return varifyedItem,errCount
}

func (cfs *ConfirmSler)ConfirmSlice() []byte{
	var resp message.SelfVarifyResp
	//cfg,err := config.ReadConfig()
	dir := util.GetYTFSPath()
	fileName := path.Join(dir, "index.db")
	fl_IdxDB, err := os.Open(fileName)
	defer fl_IdxDB.Close()

	if err != nil {
		log.Println("[confirmslice] error:", err.Error())
		return nil
	}

	header := ydcommon.Header{}
	fl_IdxDB.Seek(0, io.SeekStart)

	buf := make([]byte, unsafe.Sizeof(ydcommon.Header{}), unsafe.Sizeof(ydcommon.Header{}))
	k, err := fl_IdxDB.Read(buf)
	if (err != nil) || (k != (int)(unsafe.Sizeof(ydcommon.Header{}))) {
		log.Println("[confirmslice][error]",err)
		return nil
	}
	bufReader := bytes.NewBuffer(buf)
	err = binary.Read(bufReader, binary.LittleEndian, &header)
	if err != nil {
		log.Println("[confirmslice][error]",err)
		return nil
	}

	h := uint64(header.HashOffset)
	n := uint64(header.RangeCapacity)
    m := uint64(header.RangeCoverage)
    str_pos,_ := slicecompare.GetValueFromFile(VarifyedNumFile)
    start_pos,_ := strconv.ParseUint(str_pos,10,32)
	 cfs.SliceHashVarify(n, m, h, start_pos, fl_IdxDB)
//    varyfiedNum,errCount := cfs.SliceHashVarify(n, m, h, start_pos, fl_IdxDB)
    //resp.Numth = strconv.FormatUint(varyfiedNum,10)
    //resp.ErrNum = strconv.FormatUint(errCount,10)
    //resp.Id = strconv.FormatUint(uint64(cfg.IndexID),10)
	resp.Numth = strconv.FormatUint(3000,10)
	resp.ErrNum = strconv.FormatUint(9,10)
	resp.Id = strconv.FormatUint(183,10)
    resData,_ := proto.Marshal(&resp)
    //if err != nil {
	//	log.Println("[confirmslice][error] SliceHashVarify error!")
	//}
    return resData
}