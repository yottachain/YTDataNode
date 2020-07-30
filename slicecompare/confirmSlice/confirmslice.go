package verifySlice

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/slicecompare"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	ydcommon "github.com/yottachain/YTFS/common"
	"io"
	"os"
	"path"
	"strconv"
	"unsafe"
)

var VerifyedNumFile string = "/gc/n_file"
var hash1Str = "1111111111111111"
var hash0Str = "0000000000000000"

type VerifySler struct {
	sni.StorageNode
}

func init(){
	slicecompare.InitDir(slicecompare.SliceCompareDir)
	slicecompare.ForInit(VerifyedNumFile,"0")
}

func (vfs *VerifySler)SliceHashVarify(n, m, h, start_Item uint64, fl_IdxDB *os.File) (uint64,uint64) {
	var i uint64
	var errCount uint64
	var indexKey [16]byte
	var verifyedItem = start_Item
	n_Rangeth := start_Item/m                 //range zoom index
	m_Itermth := start_Item%m
	buf := make([]byte,20,20)
	begin := true
	for {
		log.Printf("[confirmslice] verify_parameter: n=%v,m=%v,n_Rangeth=%v", n, m, n_Rangeth)
		if n_Rangeth > (n + 1) {
			log.Println("[confirmslice] all hash in indexdb has verified, will to return!")
			slicecompare.SaveValueToFile(strconv.FormatUint(0, 10), VerifyedNumFile)
			goto OUT
		}

		pos := n_Rangeth*(4+m*20) + h + 4
		for i = 0; i < m; i++ {
			if begin {
				i = m_Itermth
				pos = pos + 20*i
				begin = false
			}
			verifyedItem++
			if verifyedItem >= start_Item+2000 {
				log.Println("[confirmslice] Has verified 2000 item, will to return!")
				slicecompare.SaveValueToFile(strconv.FormatUint(verifyedItem, 10), VerifyedNumFile)
				goto OUT
			}

			fl_IdxDB.Seek(int64(pos), io.SeekStart)
			k, err := fl_IdxDB.Read(buf)
			if (err != nil) || (k != 20) {
				fmt.Println("[confirmslice]get index error:",err)
				continue
			}

			copy(indexKey[:], buf[0:16])
			pos = pos + 20

			if base58.Encode(indexKey[:]) == hash0Str || base58.Encode(indexKey[:]) == hash1Str {
				continue
			}

			resData, err := vfs.YTFS().Get(indexKey)
			if err != nil {
				errCount++
				log.Println("[confirmslice] get data error:", err, " VHF:", base58.Encode(indexKey[:]))
				continue
			}

			if message.VerifyVHF(resData, indexKey[:]) {
				if i % 1000 == 0 {
						log.Printf("[confirmslice][hashdataok] n=%d,m=%d,n_Rangeth=%d,VHF=%s", n, m, n_Rangeth, base58.Encode(indexKey[:]))
				}
			}else{
				errCount++
				log.Println("[confirmslice][hashdataerr]verify failed,VHF:", base58.Encode(indexKey[:]))
			}
		}
		n_Rangeth++
	}
OUT:
	return verifyedItem,errCount
}

func (vfs *VerifySler)VerifySlice() []byte{
	var resp message.SelfVerifyResp
	cfg,err := config.ReadConfig()
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
    str_pos,_ := slicecompare.GetValueFromFile(VerifyedNumFile)
    start_pos,_ := strconv.ParseUint(str_pos,10,32)
    varyfiedNum,errCount := vfs.SliceHashVarify(n, m, h, start_pos, fl_IdxDB)
    resp.Numth = strconv.FormatUint(varyfiedNum,10)
    resp.ErrNum = strconv.FormatUint(errCount,10)
    resp.Id = strconv.FormatUint(uint64(cfg.IndexID),10)
    resData,_ := proto.Marshal(&resp)
    return resData
}