package confirmSlice

import (
	"bytes"
	"encoding/binary"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/slicecompare"
	stni "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	ydcommon "github.com/yottachain/YTFS/common"
	"io"
	"os"
	"path"
	"strconv"
	"unsafe"
)

var nUseForVarify string = "/gc/n_file"

type ConfirmSler struct {
	stni.StorageNode
}

func init(){
	slicecompare.InitDir(slicecompare.SliceCompareDir)
	slicecompare.ForInit(nUseForVarify,"0")
}

func (cfs *ConfirmSler)SliceHashVarify(n, m, h, nv uint64, fl *os.File) error {
	var i uint64
	var indexKey [16]byte
	ni := nv
	buf := make([]byte,16,16)

VERIFYSTA:
	log.Printf("[confirmslice] verify_parameter: n=%v,m=%v,ni=%v",n,m,ni)
	if ni >= n {
		log.Println("[confirmslice] all hash in indexdb has verified, will to return!")
		slicecompare.SaveValueToFile(strconv.FormatUint(0,10),nUseForVarify)
		return nil
	}

	if ni > nv + 200 {
		log.Println("[confirmslice] Has verified 200 zones, will to return!")
		return nil
	}

	pos := ni*(4+m*20) + h + 4
	for i = 0; i < m; i++ {
		fl.Seek(int64(pos),io.SeekStart)
		k, err := fl.Read(buf)
		if (err != nil) || (k != 16) {
			log.Printf("[confirmslice] [error] read hash from index.db to buf,k=%d,",k)
			return err
		}

		copy(indexKey[:], buf[0:16])
		pos = pos + 20

		if base58.Encode(indexKey[:]) == "1111111111111111"{
			if i % 48 == 0 {
				log.Printf("[confirmslice] the hash not valide,n=%v,m=%v,ni=%v,VHF=%v",n,m,ni,base58.Encode(indexKey[:]))
			}
			continue
		}

		resData, err := cfs.YTFS().Get(indexKey)
		if err != nil {
			log.Println("[confirmslice][error] Get data Slice fail:", err, " VHF:",base58.Encode(indexKey[:]))
		    continue
		}

		if message.VerifyVHF(resData,indexKey[:]) {
			log.Printf("[confirmslice][hashdataok] n=%d,m=%d,ni=%d,VHF=%s",n,m,ni,base58.Encode(indexKey[:]))
		}else{
			log.Println("[confirmslice][hashdataerror] data verify failed")
		}
	}

	if  ni < nv + 200  {
		ni++
		slicecompare.SaveValueToFile(strconv.FormatUint(ni,10),nUseForVarify)
		goto VERIFYSTA
	}

	return nil
}

func (cfs *ConfirmSler)ConfirmSlice() {
	dir := util.GetYTFSPath()
	fileName := path.Join(dir, "index.db")
	fl, err := os.Open(fileName)
	defer fl.Close()

	if err != nil {
		log.Println("[confirmslice] open index.db error:", err.Error())
		return
	}

	header := ydcommon.Header{}
	fl.Seek(0, io.SeekStart)

	buf := make([]byte, unsafe.Sizeof(ydcommon.Header{}), unsafe.Sizeof(ydcommon.Header{}))
	k, err := fl.Read(buf)
	if (err != nil) || (k != (int)(unsafe.Sizeof(ydcommon.Header{}))) {
		log.Println("[confirmslice][error] read header of db error, ",err)
		return
	}
	bufReader := bytes.NewBuffer(buf)
	err = binary.Read(bufReader, binary.LittleEndian, &header)
	if err != nil {
		log.Println("[confirmslice][error] read bufReader data to header error, ",err)
		return
	}

	h := uint64(header.HashOffset)
	n := uint64(header.RangeCapacity)
    m := uint64(header.RangeCoverage)
    nf,_ := slicecompare.GetValueFromFile(nUseForVarify)
    ni,_ := strconv.ParseUint(nf,10,32)
    err = cfs.SliceHashVarify(n, m, h, ni, fl)
    if err != nil {
		log.Println("[confirmslice][error] SliceHashVarify error!")
	}
    return
}