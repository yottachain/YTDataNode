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

func init(){
    slicecompare.InitDir(slicecompare.SliceCompareDir)
    slicecompare.ForInit(VerifyedNumFile,"0")
}

func (vfs *VerifySler)GetUsedEntOfRange(n, m, h, n_Rangeth uint64,  fl_IdxDB *os.File)(uint64, error){
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

func (vfs *VerifySler)TraveIndexDbForVerify(n, m, h, start_Item, traverEntries uint64, fl_IdxDB *os.File)([]ydcommon.IndexItem,error){
    var verifyTab []ydcommon.IndexItem
    var kvItem ydcommon.IndexItem
    verifyedItem := start_Item

    n_Rangeth := start_Item/m                 //range zoom index
    m_Itermth := start_Item%m
    buf := make([]byte,20,20)
    begin := true
    for {
        log.Printf("[confirmslice] verify_parameter: n=%v,m=%v,n_Rangeth=%v", n, m, n_Rangeth)
        if n_Rangeth >= (n + 1) {
            log.Println("[confirmslice] all hash in indexdb has verified, will to return!")
            slicecompare.SaveValueToFile(strconv.FormatUint(0, 10), VerifyedNumFile)
            break
        }

        usedSize, err := vfs.GetUsedEntOfRange(n, m, h, n_Rangeth, fl_IdxDB)
        if err != nil || usedSize > m{
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

            if verifyedItem >= start_Item+traverEntries {
                log.Println("[confirmslice] Has verified 2000 item, will to return!")
                slicecompare.SaveValueToFile(strconv.FormatUint(verifyedItem, 10), VerifyedNumFile)
                break
            }

            verifyedItem++

            fl_IdxDB.Seek(int64(pos), io.SeekStart)
            k, err := fl_IdxDB.Read(buf)
            if (err != nil) || (k != 20) {
                fmt.Println("[confirmslice]get index error:", err)
                continue
            }

            copy(kvItem.Hash[:], buf[0:16])
            if base58.Encode(kvItem.Hash[:]) == hash0Str || base58.Encode(kvItem.Hash[:]) == hash1Str {
                continue
            }
            kvItem.OffsetIdx = ydcommon.IndexTableValue(binary.LittleEndian.Uint32(buf[16:20]))
            verifyTab = append(verifyTab, kvItem)
            pos = pos + 20
        }
        n_Rangeth++
    }
    return verifyTab, nil
}

func (vfs *VerifySler)SliceHashVarify(n, m, h, start_Item, traverEntries uint64, fl_IdxDB *os.File) (uint64,[]*message.HashToHash,error) {
    var verifyedItem = start_Item
    var hashTab []*message.HashToHash

    verifyTab, err := vfs.TraveIndexDbForVerify(n, m, h, start_Item, traverEntries, fl_IdxDB)
    if err != nil || len(verifyTab) == 0 {
        err := fmt.Errorf("verifyTab is nil")
        return 0, nil, err
    }

    sort.Slice(verifyTab, func(i, j int) bool {
        return verifyTab[i].OffsetIdx < verifyTab[j].OffsetIdx
    })

    for _, v := range verifyTab {
        ret,err := vfs.Sn.YTFS().VerifySliceOne(v.Hash)
        if err != nil {
            var errHash message.HashToHash
            errHash.Datahash = ret.Datahash
            errHash.DBhash = ret.DBhash
            hashTab = append(hashTab,&errHash)
        }
    }
    return verifyedItem, hashTab, nil
}

func (vfs *VerifySler)VerifySliceIdxdb(travelEntries uint64) (message.SelfVerifyResp){
    var resp message.SelfVerifyResp
    cfg,err := config.ReadConfig()
    dir := util.GetYTFSPath()
    fileName := path.Join(dir, "index.db")
    fl_IdxDB, err := os.Open(fileName)
    defer fl_IdxDB.Close()

    if err != nil {
        log.Println("[verifyslice] error:", err.Error())
        resp.ErrCode="102"
        return resp
    }

    header := ydcommon.Header{}
    fl_IdxDB.Seek(0, io.SeekStart)

    buf := make([]byte, unsafe.Sizeof(ydcommon.Header{}), unsafe.Sizeof(ydcommon.Header{}))
    k, err := fl_IdxDB.Read(buf)
    if (err != nil) || (k != (int)(unsafe.Sizeof(ydcommon.Header{}))) {
        log.Println("[verifyslice] error:",err)
        resp.ErrCode = "103"
        return resp
    }
    bufReader := bytes.NewBuffer(buf)
    err = binary.Read(bufReader, binary.LittleEndian, &header)
    if err != nil {
        log.Println("[confirmslice] error:",err)
        resp.ErrCode = "103"
        return resp
    }

    h := uint64(header.HashOffset)
    n := uint64(header.RangeCapacity)
    m := uint64(header.RangeCoverage)
    str_pos,_ := slicecompare.GetValueFromFile(VerifyedNumFile)
    start_pos,_ := strconv.ParseUint(str_pos,10,32)
    varyfiedNum,hashTab,_ := vfs.SliceHashVarify(n, m, h, start_pos, travelEntries, fl_IdxDB)
    resp.Entryth = strconv.FormatUint(varyfiedNum,10)
    resp.ErrShard = hashTab
    resp.ErrNum = strconv.FormatUint(uint64(len(hashTab)),10)
    resp.Id = strconv.FormatUint(uint64(cfg.IndexID),10)
    resp.ErrCode = "000"
    return resp
}
