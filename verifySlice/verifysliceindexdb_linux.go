package verifySlice

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/slicecompare"
	"github.com/yottachain/YTDataNode/util"
	ydcommon "github.com/yottachain/YTFS/common"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"unsafe"
)

var VerifyedNumFile string = "/gc/idx_file"
var hash1Str = "1111111111111111"
var hash0Str = "0000000000000000"

func init() {
	slicecompare.InitDir(slicecompare.SliceCompareDir)
	slicecompare.ForInit(VerifyedNumFile, "0")
}

func (vfs *VerifySler) GetUsedEntOfRange(n, m, h, n_Rangeth uint64, fl_IdxDB *os.File) (uint64, error) {
	buf := make([]byte, 4)
	pos := n_Rangeth*(4+m*20) + h
	fl_IdxDB.Seek(int64(pos), io.SeekStart)
	k, err := fl_IdxDB.Read(buf)
	if (err != nil) || (k != 4) {
		fmt.Println("[confirmslice]get index error:", err)
		return 0, err
	}
	size := binary.LittleEndian.Uint32(buf)
	return uint64(size), nil
}

func (vfs *VerifySler) TraveIndexDbForVerify(n, m, h, start_Item, traverEntries uint64, fl_IdxDB *os.File) ([]ydcommon.IndexItem, error) {
	var verifyTab []ydcommon.IndexItem
	var kvItem ydcommon.IndexItem
	var verifyedItem uint64 = 0
	totalverify := start_Item

	n_Rangeth := start_Item / m //range zoom index
	m_Itermth := start_Item % m
	buf := make([]byte, 20, 20)
	begin := true
	for {
		//log.Printf("[confirmslice] verify_parameter: n=%v,m=%v,n_Rangeth=%v", n, m, n_Rangeth)
		if n_Rangeth >= (n + 1) {
			log.Println("[confirmslice] all hash in indexdb has verified, will to return!")
			slicecompare.SaveValueToFile(strconv.FormatUint(0, 10), VerifyedNumFile)
			break
		}

		usedSize, err := vfs.GetUsedEntOfRange(n, m, h, n_Rangeth, fl_IdxDB)
		//fmt.Println("[debug][verify] n=",n, "n_Ranges=",n_Rangeth,"m=",m,"usedSize=",usedSize)

		if err != nil || usedSize > m {
			n_Rangeth++
			continue
		}

		pos := n_Rangeth*(4+m*20) + h + 4
		for i := uint64(0); i < usedSize; i++ {
			if begin {
				i = m_Itermth
				pos = pos + 20*i
				begin = false
			}

			if verifyedItem >= traverEntries {
				log.Printf("[confirmslice] Has verified %d item, will to return!", traverEntries)
				slicecompare.SaveValueToFile(strconv.FormatUint(totalverify, 10), VerifyedNumFile)
				return verifyTab, nil
			}

			verifyedItem++
			totalverify++
			_, _ = fl_IdxDB.Seek(int64(pos), io.SeekStart)
			k, err := fl_IdxDB.Read(buf)
			if (err != nil) || (k != 20) {
				fmt.Println("[confirmslice][verify] get index error:", err)
				continue
			}

			copy(kvItem.Hash.Hsh[:], buf[0:16])
			if base58.Encode(kvItem.Hash.Hsh[:]) == hash0Str || base58.Encode(kvItem.Hash.Hsh[:]) == hash1Str {
				continue
			}
			kvItem.OffsetIdx = ydcommon.IndexTableValue(binary.LittleEndian.Uint32(buf[16:20]))
			verifyTab = append(verifyTab, kvItem)
			pos = pos + 20
		}
		totalverify = totalverify + m - usedSize
		n_Rangeth++
	}
	return verifyTab, nil
}

func (vfs *VerifySler) SliceHashVarify(n, m, h, start_Item, traverEntries uint64,
	fl_IdxDB *os.File) (uint64, []*message.HashToHash, error) {
	var verifyedItem = start_Item
	var hashTab []*message.HashToHash

	verifyTab, err := vfs.TraveIndexDbForVerify(n, m, h, start_Item, traverEntries, fl_IdxDB)
	if err != nil {
		return 0, nil, err
	}

	if len(verifyTab) == 0 {
		err = fmt.Errorf("404")
		return 0, nil, err
	}

	sort.Slice(verifyTab, func(i, j int) bool {
		return verifyTab[i].OffsetIdx < verifyTab[j].OffsetIdx
	})

	for _, v := range verifyTab {
		ret, err := vfs.Sn.YTFS().VerifySliceOne(v.Hash)
		if err != nil {
			log.Println("[verify] error:", err, "hash:", base58.Encode(v.Hash.Hsh[:]), "pos:", v.OffsetIdx)
			var errHash message.HashToHash
			errHash.Datahash = ret.Datahash
			errHash.DBhash = ret.DBhash
			v, _ := vfs.Hdb.DB.Get(vfs.Hdb.Ro, errHash.DBhash)
			if v.Exists() {
				errHash.HdbExist = true
			} else {
				errHash.HdbExist = false
			}

			hashTab = append(hashTab, &errHash)
		} else {
			//log.Println("[verify] success, hash:",base58.Encode(v.Hash[:]),"pos:",v.OffsetIdx)
		}
	}
	return verifyedItem, hashTab, nil
}

func (vfs *VerifySler) VerifySliceIdxdb(travelEntries uint32, startItem string) message.SelfVerifyResp {
	var resp message.SelfVerifyResp
	cfg, err := config.ReadConfig()
	resp.Id = strconv.FormatUint(uint64(cfg.IndexID), 10)

	dir := util.GetYTFSPath()
	fileName := path.Join(dir, "index.db")
	fl_IdxDB, err := os.Open(fileName)
	defer fl_IdxDB.Close()
	if err != nil {
		log.Println("[verifyslice] error:", err.Error())
		resp.ErrCode = "102"
		return resp
	}

	header := ydcommon.Header{}
	_, _ = fl_IdxDB.Seek(0, io.SeekStart)

	buf := make([]byte, unsafe.Sizeof(ydcommon.Header{}), unsafe.Sizeof(ydcommon.Header{}))
	k, err := fl_IdxDB.Read(buf)
	if (err != nil) || (k != (int)(unsafe.Sizeof(ydcommon.Header{}))) {
		log.Println("[verifyslice] error:", err)
		resp.ErrCode = "103"
		return resp
	}
	bufReader := bytes.NewBuffer(buf)
	err = binary.Read(bufReader, binary.LittleEndian, &header)
	if err != nil {
		log.Println("[verifyslice] error:", err)
		resp.ErrCode = "103"
		return resp
	}

	h := uint64(header.HashOffset)
	n := uint64(header.RangeCapacity)
	m := uint64(header.RangeCoverage)
	str_pos, _ := slicecompare.GetValueFromFile(VerifyedNumFile)
	start_pos, _ := strconv.ParseUint(str_pos, 10, 32)

	if len(startItem) > 0 {
		start_pos, _ = strconv.ParseUint(startItem, 10, 64)
	}

	varyfiedNum, hashTab, err := vfs.SliceHashVarify(n, m, h, start_pos, uint64(travelEntries), fl_IdxDB)
	if err != nil {
		resp.ErrCode = "200"
		if err.Error() == "404" {
			resp.ErrCode = "404"
		}
		resp.Id = strconv.FormatUint(uint64(cfg.IndexID), 10)
		resp.Entryth = strconv.FormatUint(varyfiedNum, 10)
		return resp
	}
	resp.Entryth = strconv.FormatUint(varyfiedNum, 10)
	resp.ErrShard = hashTab
	resp.ErrNum = strconv.FormatUint(uint64(len(hashTab)), 10)
	resp.ErrCode = "000"
	return resp
}
