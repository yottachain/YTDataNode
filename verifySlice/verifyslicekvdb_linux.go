package verifySlice

import (
    "fmt"
    "github.com/mr-tron/base58"
    log "github.com/yottachain/YTDataNode/logger"
    "github.com/yottachain/YTDataNode/slicecompare"
    "os"

    //ytfs "github.com/yottachain/YTFS"
    "strconv"
    //"github.com/yottachain/YTFS"
    "github.com/tecbot/gorocksdb"
    //"github.com/yottachain/YTDataNode/config"
    "github.com/yottachain/YTDataNode/message"
    "github.com/yottachain/YTDataNode/util"
)

var mdbFileName = "/maindb"
var VerifyedKvFile string = "/gc/rock_verify"

const Verifyhashdb  = "/verifydir/hashdb"
const Batchdb = "/verifydir/batchdb"
const Verifyrstdir = "/verifydir/rstdir/"
const VerifyBchKey = "verify_times_count_key"

func (vfs *VerifySler) Scankvdb(){
   /*todo later*/
}

func ChkRsvSpace(dir string, size int64) error {
    status_exist, _ := util.PathExists(dir)
    if !status_exist {
        os.MkdirAll(dir, 0666)
    }
    testfile := dir + "/testfile"
    fil, err :=os.OpenFile(testfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
    if err != nil{
        return err
    }

    _, err = fil.Seek(size,0)
    if err != nil{
        return err
    }

    _, err = fil.Write([]byte("end"))
    if err != nil{
        return err
    }

     os.Remove(testfile)

    return nil
}

func OpenKVDB(dbname string) (*VrDB, error) {
    dir := util.GetYTFSPath()
    DBPath := dir + dbname

    err := ChkRsvSpace(dir, 1048576)
    if err != nil{
        log.Println("[verify] ChkRsvSpace error:",err.Error())
        return nil, err
    }

    _, err = os.Stat(DBPath)
    if err != nil{
        os.MkdirAll(DBPath,0666)
    }

    bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
    bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
    opts := gorocksdb.NewDefaultOptions()
    opts.SetBlockBasedTableFactory(bbto)
    opts.SetCreateIfMissing(true)

    db, err := gorocksdb.OpenDb(opts, DBPath)
    if err != nil {
        fmt.Println("[kvdb] open rocksdb error")
        return nil, err
    }
    ro := gorocksdb.NewDefaultReadOptions()
    wo := gorocksdb.NewDefaultWriteOptions()
    VDB := &VrDB{
        db,
        ro,
        wo,
    }

    return VDB, nil
}


func (vfs *VerifySler)VerifySlicekvdb(traveEntries uint64, startItem string) (message.SelfVerifyResp){
    var resp message.SelfVerifyResp
    resp.Id = strconv.FormatUint(uint64(vfs.Sn.Config().IndexID),10)

    startkey, err := slicecompare.GetValueFromFile(VerifyedKvFile)
    if len(startItem) > 0 {
        startkey = startItem
    }

    hashTab, beginKey, err := vfs.Sn.YTFS().VerifySlice(startkey, traveEntries)
    slicecompare.SaveValueToFile(beginKey, VerifyedKvFile)
    if err != nil {
        resp.ErrCode = "200"
        log.Println("[verify] error:", err)
        return resp
    }

    log.Println("[verify] len_hashTab=", len(hashTab))
    for i:= 0; i < len(hashTab); i++ {
        var errhash message.HashToHash
        errhash.DBhash = hashTab[i].DBhash
        errhash.Datahash = hashTab[i].Datahash
        v, _ := vfs.Hdb.DB.Get(vfs.Hdb.Ro, errhash.DBhash)
        if  v.Exists(){
            continue
        }

        resp.ErrShard = append(resp.ErrShard, &errhash)
        log.Println("[verify] errhash.DBhash=",base58.Encode(hashTab[i].DBhash),
                "Datahash=",base58.Encode(hashTab[i].Datahash))
    }

    resp.ErrNum = strconv.FormatUint(uint64(len(resp.ErrShard)),10)
    resp.Entryth = startkey
    resp.ErrCode = "000"
    return resp
}
