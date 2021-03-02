package verifySlice

import (
    "fmt"
    //"github.com/yottachain/YTDataNode/config"
    "github.com/yottachain/YTDataNode/message"
    "github.com/tecbot/gorocksdb"
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


func (vfs *VerifySler)VerifySlicekvdb() (message.SelfVerifyResp){
    var resp message.SelfVerifyResp

    //vfs.YTFS().db.

    return resp
}
