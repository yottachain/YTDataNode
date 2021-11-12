package main

import (
    "context"
    "encoding/binary"
    "flag"
    "fmt"
    "github.com/golang/protobuf/proto"
    "github.com/mr-tron/base58"
    log "github.com/yottachain/YTDataNode/logger"
    "github.com/yottachain/YTHost/clientStore"
    "strconv"

    //"github.com/yottachain/YTHost/option"
    "github.com/yottachain/YTHost/service"
    "github.com/yottachain/YTHost/stat"
    "net/http"
    "net/rpc"

    //"github.com/graydream/YTHost/hostInterface"
    "github.com/yottachain/YTDataNode/config"
    "github.com/yottachain/YTDataNode/message"

    //host "github.com/yottachain/YTHost"
    //hostInterface "github.com/yottachain/YTHost/interface"

    "github.com/libp2p/go-libp2p-core/peer"
    "github.com/multiformats/go-multiaddr"
    //"fmt"
    //"github.com/mr-tron/base58"
    "github.com/yottachain/YTDataNode/verifySlice"

    //"github.com/yottachain/YTDataNode/config"
    "github.com/yottachain/YTDataNode/gc"
    "github.com/yottachain/YTDataNode/instance"
    //"github.com/yottachain/YTHost/clientStore"
    mnet "github.com/multiformats/go-multiaddr-net"

    "sync"

    "github.com/tecbot/gorocksdb"
    ydcommon "github.com/yottachain/YTFS/common"
    //"path"
    "time"
    //"github.com/mr-tron/base58/base58"
    //sni "github.com/yottachain/YTDataNode/storageNodeInterface"
)

var Mdb *KvDB
var CntPerBatch string
var StartItem string
var BatchCnt string
var VerifyErrKey  string

type Host2 struct {
    cfg      *config.Config
    listener mnet.Listener
    srv      *rpc.Server
    service.HandlerMap
    clientStore *clientStore.ClientStore
    httpClient  *http.Client
    Cs *stat.ConnStat
}

type KvDB struct {
    Rdb *gorocksdb.DB
    ro  *gorocksdb.ReadOptions
    wo  *gorocksdb.WriteOptions
    PosKey ydcommon.IndexTableKey
    PosIdx ydcommon.IndexTableValue
}

type addrInfo struct {
    DnNum	  uint32
    NodeID    peer.ID
    Addrs 	  []multiaddr.Multiaddr
}

var DelLock sync.Mutex
var delshardhash [][]byte
var gcw gc.GcWorker

func ConnRetry(ctx context.Context, maAddr multiaddr.Multiaddr, d *mnet.Dialer) (mnet.Conn, error){
    n := 0
    for{
        conn, err := d.DialContext(ctx, maAddr)
        if err != nil{
            fmt.Println("DialContext error:",err,"retry n=",n)
        }else {
            fmt.Println("Connect success")
            return conn, err
        }

        n++
        if n >= 10{
            fmt.Println("[verifytool] retry to max")
            return nil, err
        }
    }
}

func RPCRequestCommon( MsgId int32, ReqData []byte)(service.Response, error){
    var res service.Response
    n := 0
    d := &mnet.Dialer{}
    //conn := mnet.Conn{}
    ctx,cancel := context.WithTimeout(context.Background(), time.Second * 30)
    defer cancel()
    maAddr,_ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9001")
    conn, err := d.DialContext(ctx, maAddr)
    if err != nil{
        fmt.Println("DialContext error:",err,"retry n=",n)
        conn,err = ConnRetry(ctx, maAddr,d)
    }

    if nil == conn {
        fmt.Println("[verfiytool] DialContext error:",err,"retry n=",n)
        err = fmt.Errorf("connect failed")
        return res, err
    }

    pi := service.PeerInfo{}

    req := service.Request{MsgId,ReqData,pi}
    clt := rpc.NewClient(conn)
    err = clt.Call("ms.HandleMsg", req, &res)
    if err != nil{
        fmt.Println("[verifytool] err:",err)
        return res, err
    }
    return res, nil
}

func SendCompareVerifyOrder2(StartItem, CntPerBatch string){
    var respMsg message.SelfVerifyResp
    var reqMsg  message.SelfVerifyReq

    n := 0
    d := &mnet.Dialer{}
    //conn := mnet.Conn{}
    ctx,cancel := context.WithTimeout(context.Background(), time.Second * 30)
    defer cancel()
    maAddr,_ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9001")
    conn, err := d.DialContext(ctx, maAddr)
    if err != nil{
        fmt.Println("DialContext error:",err,"retry n=",n)
        conn,err = ConnRetry(ctx, maAddr,d)
    }

    if nil == conn {
       fmt.Println("[verfiytool] DialContext error:",err,"retry n=",n)
       return
    }

    reqMsg.Num = CntPerBatch
    reqMsg.StartItem = StartItem
    reqData,err := proto.Marshal(&reqMsg)
    if err != nil{
        fmt.Println("request msg error：",err.Error())
        return
    }
    pi := service.PeerInfo{}
    var res service.Response
    req := service.Request{message.MsgIDSelfVerifyReq.Value(),reqData,pi}
    clt := rpc.NewClient(conn)
    err = clt.Call("ms.HandleMsg", req, &res)
    if err != nil{
        fmt.Println("[verifytool] err:",err)
        return
    }

    err = proto.Unmarshal(res.Data[2:],&respMsg)
    if err != nil{
        fmt.Println("[verifytool] Unmarsharl err:",err.Error())
        return
    }
    fmt.Println("response nodeid:",respMsg.Id,"table idx:",respMsg.Entryth,"err account:",respMsg.ErrNum,"errCode:",respMsg.ErrCode)
    for i := 0;i < len(respMsg.ErrShard);i++{
        fmt.Println("DBHash=",base58.Encode(respMsg.ErrShard[i].DBhash),"DataHash=",base58.Encode(respMsg.ErrShard[i].Datahash),"errshard=",i)
    }
    //fmt.Println("[verifytool] respMsg:",respMsg)
}

func SelfVerifyRPC(StartItem, CntPerBatch string){
    //n := uint64(0)

    //for{
        //if n >= vTimes{
        //    fmt.Println("verifyed batchs :",n,"BatchCnt=",BatchCnt)
        //    break
        //}

        SendCompareVerifyOrder2(StartItem, CntPerBatch)
    //    <-time.After(time.Second * 2)
    //    n++
    //}
}

func MissSliceQuery(Key string){
   var req message.SelfVerifyQueryReq
   var res message.SelfVerifyQueryResp
   req.Key = Key
   reqdata, err := proto.Marshal(&req)
   if err != nil{
       fmt.Println("Marshal request error:", err)
       return
   }
   resdata, err := RPCRequestCommon(message.MsgIDSelfVerifyQueryReq.Value(),reqdata)
    if err != nil{
        fmt.Println("[verifytool] err:",err)
        return
    }

    err = proto.Unmarshal(resdata.Data[2:],&res)
    if err != nil{
        fmt.Println("Unmarshal resdata error:",err)
        return
    }
    fmt.Println(" ")
    fmt.Println("Key:",res.Key, "BatchNum:",res.BatchNum, "Date:",res.Date, "ErrCode:",res.ErrCode)
}

func ReInit(vfer *verifySlice.VerifySler){
    var err error
    vfer.Hdb, err = verifySlice.OpenKVDB(verifySlice.Verifyhashdb)
    if err != nil{
        fmt.Println("Open verify-failed hash db error:",err)
        return
    }

    vfer.Bdb, err = verifySlice.OpenKVDB(verifySlice.Batchdb)
    if err != nil{
        fmt.Println("Open verify-failed hash db error:",err)
        return
    }
}

func GetKeyStatus(vfer *verifySlice.VerifySler, SKey string){
    HKey, err := base58.Decode(SKey)
    if err != nil {
        fmt.Println("Decode key error:",err)
        return
    }

    VrfBch,err := vfer.Hdb.DB.Get(vfer.Hdb.Ro, HKey)
    if err != nil {
        fmt.Println("Get BatchNum of ",SKey," error",err.Error())
        return
    }

    if !VrfBch.Exists(){
        fmt.Println("error, hash not exist, key:",SKey,"vrfbch",VrfBch)
        return
    }
    Bbch := VrfBch.Data()
    UIBch := binary.LittleEndian.Uint64(Bbch)

    VrfTm, err := vfer.Bdb.DB.Get(vfer.Bdb.Ro, Bbch)
    if err != nil {
        fmt.Println("Get Verify-time of ",UIBch," error",err.Error())
        return
    }

    if !VrfTm.Exists(){
        fmt.Println("error, batchnum not exist, batchnum:",UIBch)
        return
    }

    STime := string(VrfTm.Data())

    fmt.Println("")
    fmt.Println("[query result] Hash:",SKey, "BatchNum:",UIBch, "Time:",STime)
}

func main(){
    Online := flag.Bool("online",true,"run verifytool online or offline")
    Loopm := flag.Bool("loop",true,"verify mode :loop or not")
    flag.StringVar(&StartItem,"s","","start items to verify")
    flag.StringVar(&CntPerBatch,"c","1000","verify items for one batch")
    flag.StringVar(&BatchCnt,"b","1000","batch count for verify")
    flag.StringVar(&VerifyErrKey,"chk","","Get verify status for verified-error key")
    flag.Parse()

    vTimes,err := strconv.ParseUint(BatchCnt,10,64)
    if err != nil{
        fmt.Println("[verifytool] error:",err)
        return
    }

    begin := true
    if *Online {
        if VerifyErrKey !=""{
            fmt.Println("check Key:",VerifyErrKey)
            MissSliceQuery(VerifyErrKey)
            return
        }
        BchCnt := uint64(0)
        for{
            for{
                BchCnt++
                SendCompareVerifyOrder2(StartItem, CntPerBatch)
                //SelfVerifyRPC(StartItem,CntPerBatch,vTimes)
                <- time.After(time.Second * 1)
                if begin{
                    log.Println("verify start!!")
                    begin = false
                    StartItem = ""
                }

                if BchCnt >= vTimes{
                    break
                }
            }

            if !*Loopm {
                break
            }
        }
    }else{
        sn := instance.GetStorageNode()
        gcw = gc.GcWorker{sn}
        vfer := verifySlice.NewVerifySler(sn)

        if VerifyErrKey !=""{
            ReInit(vfer)
            GetKeyStatus(vfer, VerifyErrKey)
            return
        }

        bchCnt := uint64(0)
        for{
            for{
                <- time.After(time.Second * 1)
                vfer.VerifySlice(CntPerBatch, StartItem)
                if begin{
                    log.Println("verify start!!")
                    begin = false
                    StartItem = ""
                }
                bchCnt++
                if bchCnt >= vTimes{
                    break
                }
            }

            if !*Loopm{
                break
            }
        }
    }
}

