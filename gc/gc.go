package gc

import (
    "fmt"
    "github.com/gogo/protobuf/proto"
    "github.com/mr-tron/base58"
    log "github.com/yottachain/YTDataNode/logger"
    "github.com/yottachain/YTDataNode/message"
    sni "github.com/yottachain/YTDataNode/storageNodeInterface"
    "github.com/yottachain/YTDataNode/util"
    ydcommon "github.com/yottachain/YTFS/common"
    "io/ioutil"
    "os"
)

/* errcode and status
000   success
100   Unmarshal gcreq error
101   marshal gcstatusresp error
200   gc process error
201   read status file error
222   not use rocksdb
333   status file not exist(maybe task was running or some error happened)
*/

type GcWorker struct{
    Sn sni.StorageNode
}

const GcDir  = "/gcstatus/"

func init(){
    filePath := util.GetYTFSPath() + GcDir
    status_exist,_ := util.PathExists(filePath)
    if status_exist == false {
        err := os.Mkdir(filePath, os.ModePerm)
        if err != nil {
            fmt.Printf("mkdir failed![%v]\n", err)
        } else {
            fmt.Printf("mkdir success!\n")
        }
    }
}

func SavetoFile(filepath string,value []byte) error{
    err := ioutil.WriteFile(filepath, value,0666)
    if err != nil{
        fmt.Println("[gcdel] save value to file error",err,"filepath:",filepath)
    }
    return err
}

func (gc *GcWorker)GcHandle(msg message.GcReq) {
    var err error
    var res message.GcStatusResp
    res.Status = "000"
    res.TaskId = msg.TaskId
    filePath := util.GetYTFSPath() + GcDir + msg.TaskId

    log.Println("[gcdel] start, taskid:",msg.TaskId)
    config := gc.Sn.Config()
    if ! config.UseKvDb {
        err = fmt.Errorf("not support indexdb for gc")
        log.Println("[gcdel] error:",err,"taskid:",msg.TaskId)
        res.Status = "222"
        value,err := proto.Marshal(&res)
        if err != nil{
            fmt.Println("[gcdel] Marshal gcstatusresp error:",err,"taskid:",msg.TaskId)
            return
        }
        err = SavetoFile(filePath,value)
        if err != nil{
            fmt.Println("[gcdel] save gcstatusresp to file error:",err,"taskid:",msg.TaskId)
        }
        return
    }

    fmt.Println("[gcdel][gclist] len_Gclist=",len(msg.Gclist))
    for _, ent := range msg.Gclist {
        fmt.Println("[gcdel][gclist] base58=",base58.Encode(ent), "string=",string(ent))
        err = gc.GcHashProcess(ent)
        if err != nil{
            log.Println("[gcdel] GcHashProcess error:",err)
            res.Status = "200"
            break;
        }
    }

    value,err := proto.Marshal(&res)
    if err != nil{
        fmt.Println("[gcdel] Marshal gcstatusresp error:",err,"taskid:",msg.TaskId)
        return
    }
    err = SavetoFile(filePath,value)
    if err != nil{
        fmt.Println("[gcdel] save gcstatusresp to file error:",err,"taskid:",msg.TaskId)
    }

    return
    //return res, err
}

func (gc *GcWorker)GcHashProcess(ent []byte) error{
    var err error
    var key ydcommon.IndexTableKey
    copy(key[:],ent)
    fmt.Println("[gcdel] GcHashProcess key=",base58.Encode(key[:]))
    err = gc.Sn.YTFS().GcProcess(key)
    if err != nil{
        log.Println("[gcdel] gc error:",err)
    }
    return err
}

func (gc *GcWorker)GetGcStatus(msg message.GcStatusReq) (message.GcStatusResp){
    var res message.GcStatusResp
    res.TaskId = msg.TaskId
    filePath := util.GetYTFSPath() + GcDir + msg.TaskId
    log.Println("[gcdel] getGcStatus, taskid:",msg.TaskId)

    status_exist,_ := util.PathExists(filePath)
    if ! status_exist {
        fmt.Println("[gcdel] statusfile not exist,filepath:",filePath)
        res.Status = "333"
        return res
    }

    value, err := ioutil.ReadFile(filePath)
    if err != nil{
        fmt.Println("[gcdel] read status file error:",err,"filepath:",filePath)
        res.Status = "201"
        return res
    }

    err = proto.Unmarshal(value,&res)
    if err !=nil{
        fmt.Println("[gcdel] unmarshal statusfile to resp error:",err,"filepath:",filePath)
        res.Status = "201"
    }

    return res
}