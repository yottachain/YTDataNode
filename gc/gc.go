package gc

import (
    "fmt"
    "github.com/gogo/protobuf/proto"
    "github.com/mr-tron/base58"
    "github.com/yottachain/YTDataNode/config"
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
const GcCleanKey = "_gc_clean_key_"

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
    status_exist,_ := util.PathExists(filepath)
    if status_exist {
        _ = os.Remove(filepath)
    }

    err := ioutil.WriteFile(filepath, value,0666)
    if err != nil{
        fmt.Println("[gcdel] save value to file error",err,"filepath:",filepath)
    }
    return err
}

func (gc *GcWorker)GcMsgChkHdl(data []byte) (message.GcResp, error) {
    var msg message.GcReq
    var res message.GcResp

    res.Dnid = gc.Sn.Config().IndexID

    if err := proto.Unmarshal(data, &msg); err != nil {
        log.Println("[gcdel] message.GcReq error:", err)
        res.ErrCode = "errReq"
        res.TaskId = "nil"
        err := fmt.Errorf("errReq")
        return res, err
    }

    if !config.Gconfig.GcOpen{
        res.ErrCode = "errNotOpenGc"
        err := fmt.Errorf("errNotOpenGc")
        return  res, err
    }

    if err := gc.GcTestSysSpace(msg); err != nil {
        log.Println("[gcdel] message.GcReq error:", err)
        res.ErrCode = "errnospace"
        res.TaskId = "nil"
        err := fmt.Errorf("errnospace")
        return res, err
    }

    if msg.Dnid != gc.Sn.Config().IndexID {
        log.Println("[gcdel] message.GcReq error")
        res.ErrCode = "errNodeid"
        res.TaskId = "nil"
        err := fmt.Errorf("errNodeid")
        return res, err
    }

    go gc.GcHandle(msg)

    res.TaskId = msg.TaskId
    res.ErrCode = "succ"
    return res, nil
}

func (gc *GcWorker)GcHandle(msg message.GcReq) {
    var err error
    var res message.GcStatusResp
    res.Status = "succ"
    res.TaskId = msg.TaskId
    filePath := util.GetYTFSPath() + GcDir + msg.TaskId

    log.Println("[gcdel] start, taskid:", msg.TaskId)
    config := gc.Sn.Config()
    if !config.UseKvDb {
        err = fmt.Errorf("not support indexdb for gc")
        log.Println("[gcdel] error:", err, "taskid:",msg.TaskId)
        res.Status = "DBcfgerr"
        value,err := proto.Marshal(&res)
        if err != nil{
            fmt.Println("[gcdel] Marshal gcstatusresp error:", err, "taskid:",msg.TaskId)
            return
        }
        err = SavetoFile(filePath, value)
        if err != nil{
            fmt.Println("[gcdel] save gcstatusresp to file error:", err, "taskid:", msg.TaskId)
        }
        return
    }

    fmt.Println("[gcdel][gclist] len_Gclist=", len(msg.Gclist))

    for _, ent := range msg.Gclist {
        fmt.Println("[gcdel][gclist] base58=",base58.Encode(ent), "string=",string(ent))
        err = gc.GcHashProcess(ent)
        if err != nil{
            log.Println("[gcdel] GcHashProcess error:",err)
            res.Status = "parterr"
            res.Fail++
            res.Errlist = append(res.Errlist, ent)
        }
        res.Total++
    }

    if res.Fail == res.Total{
        res.Status = "allerr"
    }

    value,err := proto.Marshal(&res)
    if err != nil{
        fmt.Println("[gcdel] Marshal gcstatusresp error:",err,"taskid:",msg.TaskId)
        return
    }
    err = SavetoFile(filePath, value)
    if err != nil{
        fmt.Println("[gcdel] save gcstatusresp to file error:",err,"taskid:",msg.TaskId)
    }
    return
}

func (gc *GcWorker) GcTestSysSpace(msg message.GcReq) error {
    var res message.GcStatusResp
    var err error
    res.Status = "succ"
    res.TaskId = msg.TaskId
    filePath := util.GetYTFSPath() + GcDir + msg.TaskId
    value, err := proto.Marshal(&res)
    if err != nil{
        fmt.Println("[gcdel] Marshal gcstatusresp error:", err, "taskid:", msg.TaskId)
        return err
    }

    err = SavetoFile(filePath, value)
    if err != nil{
        fmt.Println("[gcdel] save gcstatusresp to file error:", err, "taskid:", msg.TaskId)
    }
    return err
}

func (gc *GcWorker)GcHashProcess(ent []byte) error{
    var err error
    var key ydcommon.IndexTableKey
    entstr := string(ent)
    k,err := base58.Decode(entstr)
    if err != nil{
        fmt.Println("[gcdel] decode hashstr error:",err)
        return err
    }

    copy(key[:],k)
    fmt.Println("[gcdel] GcHashProcess key=",base58.Encode(key[:]))
    err = gc.Sn.YTFS().GcProcess(key)
    if err != nil{
        log.Println("[gcdel] gc error:",err)
    }
    return err
}

func (gc *GcWorker)GetGcStatusHdl(data []byte)(message.GcStatusResp, error){
    var msg message.GcStatusReq
    var res message.GcStatusResp
    var err error

    if err := proto.Unmarshal(data, &msg); err != nil {
        log.Println("[gcdel] message.GcReq error:", err)
        res.Status = "errstatusreq"
        return res, err
    }

    res.Dnid = gc.Sn.Config().IndexID
    if !config.Gconfig.GcOpen{
        res.Status = "errNotOpenGc"
        return res, err
    }

    if msg.Dnid != gc.Sn.Config().IndexID{
        log.Println("[gcdel] message.GcReq error:", err)
        res.Status = "errNodeid"
        res.TaskId = "nil"
        return res, err
    }

    res = gc.GetGcStatus(msg)
    return res, nil
}

func (gc *GcWorker)GetGcStatus(msg message.GcStatusReq) (message.GcStatusResp){
    var res message.GcStatusResp
    res.Status = "succ"
    res.TaskId = msg.TaskId
    filePath := util.GetYTFSPath() + GcDir + msg.TaskId
    log.Println("[gcdel] getGcStatus, taskid:",msg.TaskId)

    status_exist,_ := util.PathExists(filePath)
    if ! status_exist {
        fmt.Println("[gcdel] statusfile not exist,filepath:",filePath)
        res.Status = "nofile"
        return res
    }

    value, err := ioutil.ReadFile(filePath)
    if err != nil{
        fmt.Println("[gcdel] read status file error:",err,"filepath:",filePath)
        res.Status = "fileRdErr"
        return res
    }

    err = proto.Unmarshal(value,&res)
    if err !=nil{
        fmt.Println("[gcdel] unmarshal statusfile to resp error:",err,"filepath:",filePath)
        res.Status = "fileUnmarshalErr"
    }
    return res
}

func (gc *GcWorker)GcDelStatusFileHdl(data []byte)(message.GcdelStatusfileResp, error){
    var msg message.GcdelStatusfileReq
    var res message.GcdelStatusfileResp
    var err error
    res.Dnid = gc.Sn.Config().IndexID

    if err := proto.Unmarshal(data, &msg); err != nil {
        log.Println("[gcdel] message.GcReq error:", err)
        res.Status = "errdelreq"
        err = fmt.Errorf("errdelreq")
        return res, err
    }

    if !config.Gconfig.GcOpen{
        res.Status = "errNotOpenGc"
        err = fmt.Errorf("errNotOpenGc")
        log.Println("[gcdel] error:", err)
        return res, err
    }

    if msg.Dnid != gc.Sn.Config().IndexID {
        res.Status = "errNodeid"
        res.TaskId = "nil"
        err = fmt.Errorf("errNodeid")
        log.Println("[gcdel] error:", err)
        return res, err
    }

    res = gc.GcDelStatusfile(msg)
    return res, nil
}

func (gc *GcWorker)GcDelStatusfile(msg message.GcdelStatusfileReq) (message.GcdelStatusfileResp){
    var res message.GcdelStatusfileResp
    res.TaskId = msg.TaskId
    res.Status = "ok"

    filePath := util.GetYTFSPath() + GcDir + msg.TaskId
    log.Println("[gcdel] getGcStatus, taskid:",msg.TaskId)

    status_exist,_ := util.PathExists(filePath)
    if ! status_exist {
        fmt.Println("[gcdel] statusfile not exist,filepath:",filePath)
        res.Status = "nofile"
        return res
    }

    err := os.Remove(filePath)
    if err !=nil {
       fmt.Println("[gcdel] delete status file error:",err)
        res.Status = "delerr"
    }

    return res
}

func (gc *GcWorker) CleanGc() (succs, fails uint32){
    if !gc.Sn.Config().UseKvDb {
        return
    }

    keyData, err := gc.Sn.YTFS().YtfsDB().GetDb([]byte(GcCleanKey))
    if err != nil {
        log.Printf("[gcclean] get gc clean key err %s\n", err.Error())
        return
    }
    if keyData != nil {
        log.Println("[gcclean] get gc clean key have existed")
        return
    }else {
        log.Println("[gcclean] start")
    }

    var times uint32
    for {
        bm, _ := gc.Sn.YTFS().YtfsDB().GetBitMapTab(1000)
        if len(bm) <= 0 {
            break
        }
        suc, fail := gc.Sn.YTFS().GcDelBitMap(bm)
        succs += suc
        fails += fail
        times ++
        log.Printf("[gcclean] clean gc times: %d succs: %d fails: %d\n",
            times, succs, fails)
    }

    _ = gc.Sn.YTFS().PutGcNums(fails)

    err = gc.Sn.YTFS().YtfsDB().PutDb([]byte(GcCleanKey), []byte("gc_first"))
    if err != nil {
        log.Println("[gcclean] put key fail")
    }else {
        log.Println("[gcclean] put key succs")
    }

    return
}