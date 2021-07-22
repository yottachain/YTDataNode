package verifySlice

import (
    "fmt"
    "github.com/mr-tron/base58"
    log "github.com/yottachain/YTDataNode/logger"
    "github.com/yottachain/YTDataNode/slicecompare"
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

const verifyhashdb  = "/verifydir/hashdb"
const batchdb = "/verifydir/batchdb"


func (vfs *VerifySler) Scankvdb(){

}

func openKVDB(dbname string) (*VrDB, error) {
    //	var posIdx uint32
    //cfg,err := config.ReadConfig()
    dir := util.GetYTFSPath()
    DBPath := dir + dbname

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
    var errhash message.HashToHash
    //var hashTab []ytfs.Hashtohash

    startkey,err := slicecompare.GetValueFromFile(VerifyedKvFile)
    if len(startItem) > 0 {
        startkey = startItem
    }

    //startkey :=
    hashTab,beginKey,err := vfs.Sn.YTFS().VerifySlice(startkey,traveEntries)
    slicecompare.SaveValueToFile(beginKey, VerifyedKvFile)
    if err != nil {
        resp.ErrCode = "200"
        log.Println("[verify] error:",err)
        return resp
    }

    log.Println("[verify] len_hashTab=",len(hashTab))
    for i:= 0; i < len(hashTab); i++{
        errhash.DBhash = hashTab[i].DBhash
        errhash.Datahash = hashTab[i].Datahash
        resp.ErrShard = append(resp.ErrShard,&errhash)
        log.Println("[verify] errhash.DBhash=",base58.Encode(hashTab[i].DBhash),"Datahash=",base58.Encode(hashTab[i].Datahash))
    }
    //vfs.Sn.Config().IndexID
    resp.Id = strconv.FormatUint(uint64(vfs.Sn.Config().IndexID),10)
    resp.ErrNum = strconv.FormatUint(uint64(len(resp.ErrShard)),10)
    resp.Entryth = startkey
    //resp.ErrShard = retSlice
    resp.ErrCode = "000"
    return resp
}
