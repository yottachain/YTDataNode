package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multiaddr"
	mnet "github.com/multiformats/go-multiaddr-net"
	"github.com/natefinch/lumberjack"
	"github.com/spf13/cobra"
	"github.com/tecbot/gorocksdb"
	ci "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/instance"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/snapi"
	storage "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTDataNode/verifySlice"
	Ytfs "github.com/yottachain/YTFS"
	ydcommon "github.com/yottachain/YTFS/common"
	opt "github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTHost/service"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var Mdb *KvDB
var CntPerBatch uint32
var StartItem string
var BatchCnt uint32
var VerifyErrKey string
var Loop = true
var Online = true
var truncat = false

type KvDB struct {
	Rdb    *gorocksdb.DB
	ro     *gorocksdb.ReadOptions
	wo     *gorocksdb.WriteOptions
	PosKey ydcommon.IndexTableKey
	PosIdx ydcommon.IndexTableValue
}

func ConnRetry(maAddr multiaddr.Multiaddr, times int) (mnet.Conn, error) {
	n := 0
	d := &mnet.Dialer{}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		conn, err := d.DialContext(ctx, maAddr)
		if err != nil {
			fmt.Println("[verifytool] DialContext error:", err, "retry n=", n)
		} else {
			fmt.Println("[verifytool] Connect success")
			return conn, err
		}

		n++
		if n >= times {
			fmt.Println("[verifytool] retry to max")
			return nil, err
		}

		cancel()
	}
}

func RPCRequestCommon(MsgId int32, ReqData []byte) (service.Response, error) {
	var res service.Response

	maAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9001")
	conn, err := ConnRetry(maAddr, 10)
	if err != nil {
		fmt.Println("[verfiytool] DialContext error:", err)
	}

	if nil == conn {
		fmt.Println("[verfiytool] DialContext error:", err)
		err = fmt.Errorf("connect failed")
		return res, err
	}

	pi := service.PeerInfo{}

	req := service.Request{MsgId, ReqData, pi}
	clt := rpc.NewClient(conn)
	err = clt.Call("ms.HandleMsg", req, &res)
	if err != nil {
		fmt.Println("[verifytool] err:", err)
		return res, err
	}
	return res, nil
}

func SendCompareVerifyOrder2(StartItem string, CntPerBatch uint32) (*message.SelfVerifyResp, error) {
	var respMsg message.SelfVerifyResp
	var reqMsg message.SelfVerifyReq

	maAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9001")
	conn, err := ConnRetry(maAddr, 10)
	if nil != err {
		fmt.Println("[verfiytool] DialContext error:", err)
		return nil, err
	}

	reqMsg.Num = strconv.FormatUint(uint64(CntPerBatch), 10)
	reqMsg.StartItem = StartItem
	reqData, err := proto.Marshal(&reqMsg)
	if err != nil {
		fmt.Println("request msg error：", err.Error())
		return nil, err
	}
	pi := service.PeerInfo{}
	var res service.Response
	req := service.Request{message.MsgIDSelfVerifyReq.Value(), reqData, pi}
	clt := rpc.NewClient(conn)
	defer clt.Close()
	err = clt.Call("ms.HandleMsg", req, &res)
	if err != nil {
		fmt.Println("[verifytool] err:", err)
		return nil, err
	}

	err = proto.Unmarshal(res.Data[2:], &respMsg)
	if err != nil {
		fmt.Println("[verifytool] Unmarsharl err:", err.Error())
		return nil, err
	}

	fmt.Println("response nodeid:", respMsg.Id, "table idx:", respMsg.Entryth,
		"err account:", respMsg.ErrNum, "errCode:", respMsg.ErrCode)

	for i := 0; i < len(respMsg.ErrShard); i++ {
		fmt.Println("DBHash=", base58.Encode(respMsg.ErrShard[i].DBhash),
			"DataHash=", base58.Encode(respMsg.ErrShard[i].Datahash),
			"dataPos=", respMsg.ErrShard[i].Pos, "errshard=", i)
	}

	return &respMsg, nil
}

func SelfVerifyRPC(StartItem string, CntPerBatch uint32) {
	SendCompareVerifyOrder2(StartItem, CntPerBatch)
}

func MissSliceQuery(Key string) error {
	var req message.SelfVerifyQueryReq
	var res message.SelfVerifyQueryResp
	req.Key = Key
	reqdata, err := proto.Marshal(&req)
	if err != nil {
		fmt.Println("Marshal request error:", err)
		return err
	}
	resdata, err := RPCRequestCommon(message.MsgIDSelfVerifyQueryReq.Value(), reqdata)
	if err != nil {
		fmt.Println("[verifytool] err:", err)
		return err
	}

	err = proto.Unmarshal(resdata.Data[2:], &res)
	if err != nil {
		fmt.Println("Unmarshal resdata error:", err)
		return err
	}

	if res.ErrCode == "ErrNoHashKey" {
		fmt.Printf("key:%s not verify error record!\n", res.Key)
	} else {
		fmt.Println("Key:", res.Key, "BatchNum:", res.BatchNum,
			"Date:", res.Date, "ErrCode:", res.ErrCode)
	}

	return nil
}

func ReInit(vfer *verifySlice.VerifySler) {
	var err error
	vfer.Hdb, err = verifySlice.OpenKVDB(verifySlice.Verifyhashdb)
	if err != nil {
		fmt.Println("Open verify-failed hash db error:", err)
		return
	}

	vfer.Bdb, err = verifySlice.OpenKVDB(verifySlice.Batchdb)
	if err != nil {
		fmt.Println("Open verify-failed hash db error:", err)
		return
	}
}

func GetKeyStatus(vfer *verifySlice.VerifySler, SKey string) {
	HKey, err := base58.Decode(SKey)
	if err != nil {
		fmt.Println("Decode key error:", err)
		return
	}

	VrfBch, err := vfer.Hdb.DB.Get(vfer.Hdb.Ro, HKey)
	if err != nil {
		fmt.Println("Get BatchNum of ", SKey, " error", err.Error())
		return
	}

	if !VrfBch.Exists() {
		fmt.Println("error, hash not exist, key:", SKey, "vrfbch", VrfBch)
		return
	}
	Bbch := VrfBch.Data()
	UIBch := binary.LittleEndian.Uint64(Bbch)

	VrfTm, err := vfer.Bdb.DB.Get(vfer.Bdb.Ro, Bbch)
	if err != nil {
		fmt.Println("Get Verify-time of ", UIBch, " error", err.Error())
		return
	}

	if !VrfTm.Exists() {
		fmt.Println("error, batchnum not exist, batchnum:", UIBch)
		return
	}

	STime := string(VrfTm.Data())

	fmt.Println("")
	fmt.Println("[query result] Hash:", SKey, "BatchNum:", UIBch, "Time:", STime)
}

func SendToElk(resp *message.SelfVerifyResp, wg *sync.WaitGroup) {
	wg.Add(1)

	defer func() {
		wg.Done()
	}()

	var elkData VerifyErrShards
	id, _ := strconv.ParseInt(resp.Id, 10, 64)
	ErrNums, _ := strconv.ParseInt(resp.ErrNum, 10, 32)
	elkData.MinerId = id
	elkData.ErrNums = int32(ErrNums)
	for _, v := range resp.ErrShard {
		var errShard ErrShard
		errShard.RebuildStatus = 0
		errShard.Shard = base58.Encode(v.DBhash)
		elkData.ErrShards = append(elkData.ErrShards, errShard)
	}

	err := PutVerifyErrData(&elkData)
	if err != nil {
		log.Printf("verify put error shards fail %s\n", err.Error())
	}
}

func verifyAndTruncatYtfsStorage(ytfs *Ytfs.YTFS) {
	ytfs.TruncatStorageFile()
}

func cfgCheck() (err error) {
	cfg, err := config.ReadConfig()
	if err != nil {
		log.Printf("config err %s, verify that the configuration file is stored or correct!\n",
			err.Error())
		fmt.Printf("config err %s, verify that the configuration file is stored or correct!\n",
			err.Error())
		return
	} else {
		log.Println("verify config read success!")
		fmt.Println("verify config read success!")
	}

	eosPri, err := util.Libp2pPkey2eosPkey(cfg.PrivKeyString())
	if err != nil {
		log.Printf("pri key transform err %s\n", err.Error())
		fmt.Printf("pri key transform err %s\n", err.Error())
	}
	//verify pubkey and prikey
	pubkey, err := ci.GetPublicKeyByPrivateKey(eosPri)
	if err != nil {
		log.Printf("get pub key err %s\n", err.Error())
		fmt.Printf("get pub key err %s\n", err.Error())
		fmt.Printf("get pub key err %s\n", err.Error())
	}

	if cfg.PubKey != pubkey {
		log.Printf("verify the public and private keys do not match!\n")
		fmt.Printf("verify the public and private keys do not match!\n")
		return fmt.Errorf("the public and private keys do not match, pubkey %s, privkey %s\n",
			pubkey, cfg.PrivKeyString())
	} else {
		log.Println("verify public/private key pair success!")
		fmt.Println("verify public/private key pair success!")
	}

	//Verify that the configuration ID and database ID are consistent
	ys, err := Ytfs.OpenGet(util.GetYTFSPath(), cfg.Options, cfg.IndexID)
	if err != nil {
		log.Printf("verify open ytfs err %s\n", err.Error())
		fmt.Printf("verify open ytfs err %s\n", err.Error())
		return fmt.Errorf("verify open ytfs err %s\n", err.Error())
	}

	_, err = ys.YtfsDB().CheckDbDnId(cfg.IndexID)
	if err != nil {
		log.Printf("verify miner id err %s\n", err.Error())
		fmt.Printf("verify miner id err %s\n", err.Error())
		return fmt.Errorf("verify miner id err %s\n", err.Error())
	} else {
		log.Printf("verify config/db  miner id consistent!\n")
		fmt.Printf("verify config/db  miner id consistent!\n")
	}

	_, err = opt.FinalizeConfig(cfg.Options)
	if err != nil {
		log.Printf("verify storage options err %s\n", err.Error())
		fmt.Printf("verify storage options err %s\n", err.Error())
		return fmt.Errorf("verify storage options err %s", err.Error())
	} else {
		log.Println("verify storage options consistent!")
		fmt.Println("verify storage options consistent!")
	}

	log.Println("verify all success!")
	fmt.Println("verify all success!")

	return nil
}

func deleteHdbKeys() {
	sn := instance.GetStorageNode()
	if sn == nil {
		fmt.Printf("get storage node fail\n")
		return
	}
	vfer := verifySlice.NewVerifySler(sn)

	if vfer == nil {
		fmt.Printf("new VerifySler fail\n")
		return
	}

	vfer.TravelHDB(func(key, value []byte) error {
		Hkey := ydcommon.BytesToHash(key)
		_ = vfer.Hdb.DB.Delete(vfer.Hdb.Wo, Hkey[:])
		return nil
	})

	fmt.Println("cls hdb success!")
}

func Start() {
	wg := &sync.WaitGroup{}
	begin := true
	var sn storage.StorageNode

	go config.Gconfig.UpdateService(context.Background(), time.Minute*10)

	var vfer *verifySlice.VerifySler
	if !Online {
		sn = instance.GetStorageNode()
		if truncat {
			verifyAndTruncatYtfsStorage(sn.YTFS())
		}
		vfer = verifySlice.NewVerifySler(sn)
	}

	reportTotalErrs := uint64(0)
	verifyTotalShards := uint64(0)

	log.Printf("loop is %x, bchCnt is %d\n", Loop, BatchCnt)

	for {
		totalErrShards := uint64(100000)
		if config.Gconfig.VerifyReportMaxNum != 0 {
			totalErrShards = config.Gconfig.VerifyReportMaxNum
		}

		bchCnt := uint32(0)
		for {
			//<- time.After(time.Second * 1)
			var resp *message.SelfVerifyResp
			var err error
			if Online {
				resp, err = SendCompareVerifyOrder2(StartItem, CntPerBatch)
				if err != nil {
					log.Printf("verify batch %d errs %s\n", bchCnt, err.Error())
					continue
				}
			} else {
				if vfer == nil {
					log.Println("verify error verifySlice is nil")
					goto exit
				} else {
					resp = vfer.VerifySlice(CntPerBatch, StartItem)
				}
			}

			realVerifyNum, _ := strconv.Atoi(resp.Num)
			verifyTotalShards += uint64(realVerifyNum)

			if resp.ErrCode == "404" {
				log.Printf("verify not found, start %s\n", resp.Entryth)
				goto exit
			} else if resp.ErrCode != "000" {
				log.Printf("this round verify happen error %s\n", resp.ErrCode)
			}

			errNum := len(resp.ErrShard)
			if errNum > 0 {
				log.Printf("this round verify report err shards %d\n", errNum)
				//go SendToElk(resp, wg)
				wg.Add(1)
				go snapi.SendToSnApi(resp, wg)
				reportTotalErrs += uint64(errNum)
			}
			if begin {
				log.Println("verify start!!")
				begin = false
				StartItem = ""
			}
			bchCnt++
			if bchCnt >= BatchCnt || reportTotalErrs >= totalErrShards {
				break
			}
		}

		if !Loop || reportTotalErrs >= totalErrShards {
			break
		}
	}

exit:
	log.Printf("verify report total shards %d, err shards %d\n", verifyTotalShards, reportTotalErrs)

	wg.Wait()
}

func VerifyStatus() {
	if VerifyErrKey != "" {
		fmt.Println("verify check Key:", VerifyErrKey)
		err := MissSliceQuery(VerifyErrKey)
		if err != nil {
			sn := instance.GetStorageNode()
			vfer := verifySlice.NewVerifySler(sn)
			GetKeyStatus(vfer, VerifyErrKey)
		}
	} else {
		log.Printf("verify check key shouldn't nil")
	}
}

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "以守护进程启动程序",
	Run: func(cmd *cobra.Command, args []string) {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)

		logfile := log.NewSyncWriter(&lumberjack.Logger{
			Filename:   path.Join(util.GetYTFSPath(), "verify.log"),
			MaxSize:    128,
			Compress:   false,
			MaxAge:     7,
			MaxBackups: 1,
		})
		log.SetOutput(logfile)

		c := exec.Command(os.Args[0], "start")
		c.Env = os.Environ()
		c.Stdout = logfile
		c.Stderr = logfile
		err := c.Run()
		if err != nil {
			log.Println("进程启动失败:", err)
		} else {
			log.Println("守护进程已启动")
		}
	},
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "前台运行程序",
	Run: func(cmd *cobra.Command, args []string) {
		Start()
	},
}

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "查询校验的状态",
	Run: func(cmd *cobra.Command, args []string) {
		VerifyStatus()
	},
}

var truncatCmd = &cobra.Command{
	Use:   "truncat",
	Short: "检查存储文件的尺寸是否大于配置并截断",
	Long:  "ytfs file stroage, Check whether the file size exceeds the configured size and truncat file",
	Run: func(cmd *cobra.Command, args []string) {
		sn := instance.GetStorageNode()
		if sn == nil {
			log.Println("truncat sn open fail")
			return
		}
		verifyAndTruncatYtfsStorage(sn.YTFS())
	},
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "配置文件相关操作",
	Run: func(cmd *cobra.Command, args []string) {
		log.SetOutput(os.Stdout)
	},
}

var configCheck = &cobra.Command{
	Use:   "check",
	Short: "配置文件正确性检查",
	Long:  "check whether the configuration file is correct",
	Run: func(cmd *cobra.Command, args []string) {
		log.SetOutput(os.Stdout)
		err := cfgCheck()
		if err != nil {
			log.Printf("config verify error %s\n", err.Error())
			fmt.Printf("config verify error %s\n", err.Error())
		}
	},
}

var clsHdbCmd = &cobra.Command{
	Use:   "clsHdb",
	Short: "清空hdb的所有key",
	Run: func(cmd *cobra.Command, args []string) {
		deleteHdbKeys()
	},
}

func main() {
	startCmd.Flags().StringVarP(&StartItem, "start", "s", "start_anew",
		"assign start hash to verify")
	startCmd.Flags().Uint32VarP(&CntPerBatch, "count", "c", 1000,
		"verify items for one batch")
	startCmd.Flags().Uint32VarP(&BatchCnt, "batch", "b", 1000,
		"batch count for verify")
	startCmd.Flags().BoolVarP(&Loop, "loop", "l", true,
		"verify mode :loop or not")
	startCmd.Flags().BoolVarP(&Online, "online", "o", true,
		"run verifytool while dn online or offline, "+
			"set false will panic while dn is online")
	startCmd.Flags().BoolVarP(&truncat, "truncat", "t", false,
		"ytfs file stroage,"+
			"Check whether the file size exceeds the configured size and truncat file,"+
			"available while the dn is offline")

	checkCmd.Flags().StringVarP(&VerifyErrKey, "key", "k", "",
		"Get verify status for verified-error key")

	//log.SetFileLog()

	RootCommand := &cobra.Command{
		Short: "ytfs verify",
	}
	RootCommand.AddCommand(startCmd)
	RootCommand.AddCommand(checkCmd)
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(truncatCmd)
	RootCommand.AddCommand(clsHdbCmd)
	RootCommand.AddCommand(configCmd)
	configCmd.AddCommand(configCheck)

	RootCommand.Execute()
}
