package main

import (
    "crypto/md5"
    //"encoding/binary"
    "flag"
    "fmt"

    "github.com/mr-tron/base58"
    "github.com/yottachain/YTDataNode/config"
    "github.com/yottachain/YTDataNode/instance"
    ydcommon "github.com/yottachain/YTFS/common"
    //"github.com/yottachain/YTDataNode/verifySlice"
)

var StartPosIndex uint
var VerifyCnt uint
var HashKey string

func main() {
    flag.UintVar(&StartPosIndex, "s", 0, "Start pos to verify")
    flag.UintVar(&VerifyCnt, "c", 100, "total item for verify")
    flag.StringVar(&HashKey, "key", "", "Get data block pos of key")
    flag.Parse()

    sn := instance.GetStorageNode()

    if HashKey != "" {
        BKey, err := base58.Decode(HashKey)
        if err != nil {
            fmt.Println("Decode error:", err.Error(), "key:", HashKey)
            return
        }
        var IdxKey ydcommon.IndexTableKey
        copy(IdxKey[:], BKey[0:16])
        pos, err := sn.YTFS().GetPosIdx(IdxKey)
        if err != nil {
            fmt.Println("GetPosIdx error:", err.Error(), "key:", HashKey, "pos=", pos)
            return
        }
        fmt.Println("GetPosIdx succ, key:", HashKey, "pos=", pos)

        data, err := sn.YTFS().GetData(uint32(pos))
        if err != nil {
            fmt.Println("get data error:", err.Error(), "posidx=", pos)
            return
        }

        key := md5.Sum(data)
        if base58.Encode(key[:]) != HashKey {
            fmt.Println("data error:Hashkey=", HashKey, " datahash=", base58.Encode(key[:]), "posidx=", pos)
            return
        }
        fmt.Println("datacheck succ:Hashkey=", HashKey, "datahash=", base58.Encode(key[:]), "posidx=", pos)
        return
    }

    for n := uint(0); n < VerifyCnt; n++ {
        posidx := uint32(StartPosIndex + n)
        data, err := sn.YTFS().GetData(posidx)
        if err != nil {
            fmt.Println("get data error:", err.Error(), "posidx=", posidx)
            continue
        }

        if len(data) != (int)(config.GlobalShardSize*1024) {
            fmt.Println("get data error, len(data)=", len(data))
            continue
        }

        key := md5.Sum(data)

        pos, err := sn.YTFS().GetPosIdx(key)
        if err != nil {
            fmt.Println("GetPosIdx error:", err.Error(), "key:", base58.Encode(key[:]), "posidx=", posidx)
            continue
        }

        if uint32(pos) != posidx {
            fmt.Println("error, pos != posidx, pos=", pos, "posidx=", posidx, "key:", base58.Encode(key[:]))
            continue
        }

        fmt.Println("succ, pos == posidx, pos=", pos, "posidx=", posidx, "key:", base58.Encode(key[:]))
    }
}
