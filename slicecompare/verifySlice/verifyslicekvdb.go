package verifySlice

import (
    "fmt"
    log "github.com/yottachain/YTDataNode/logger"
    "github.com/yottachain/YTDataNode/slicecompare"
    "strconv"

    "github.com/tecbot/gorocksdb"
    //"github.com/yottachain/YTDataNode/config"
    "github.com/yottachain/YTDataNode/message"
    "github.com/yottachain/YTDataNode/util"
)

var mdbFileName = "/maindb"

func (vfs *VerifySler) Scankvdb(){

}

func openKVDB() (*gorocksdb.DB, error) {
    //	var posIdx uint32
    //cfg,err := config.ReadConfig()
    dir := util.GetYTFSPath()
    DBPath := dir + mdbFileName

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
    return db, nil
    //ro := gorocksdb.NewDefaultReadOptions()
    //wo := gorocksdb.NewDefaultWriteOptions()
}


func (vfs *VerifySler)VerifySlicekvdb(traveEntries uint64) (message.SelfVerifyResp){
    var resp message.SelfVerifyResp
    startkey,err := slicecompare.GetValueFromFile(VerifyedNumFile)
    //startkey :=
    retSlice,err := vfs.Sn.YTFS().VerifySlice(startkey,traveEntries)
    if err != nil {
        resp.ErrCode = "200"
        log.Println("[verify] error:",err)
        return resp
    }

    resp.ErrNum = strconv.FormatUint(uint64(len(retSlice)),10)
    resp.ErrShard = retSlice
    resp.ErrCode = "000"
    return resp
}
