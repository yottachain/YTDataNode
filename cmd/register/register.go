package registerCmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"

	//"github.com/eoscanada/eos-go/ecc"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var baseNodeUrl = "http://dnapi1.yottachain.net:8888" //正式
//var baseNodeUrl = "http://124.156.54.96:8888" //测试

var api = eos.New(baseNodeUrl)
var BPList []string
var bi int
var minerid uint64
var adminacc string
var depAcc string
var depAmount int64 = 1024 * 1024 * 1024 * 10
var kb = eos.NewKeyBag()
var maxSpace uint64 = 268435456

var isDebug = false

var yOrN byte

var RegisterCmd = &cobra.Command{
	Short: "注册账号",
	Use:   "register",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			baseNodeUrl = args[0]
			for _, bp := range args {
				BPList = append(BPList, bp)
			}
		}
		fmt.Println("bplist", BPList)
		initConfig, err := readCfg()
		if err != nil || initConfig.IndexID == 0 {
			fmt.Println("未查询到矿机注册信息，是否重新注册：y/n?")
			fmt.Scanf("%c\n", &yOrN)
			if yOrN == 'y' {
				fmt.Println(yOrN)
				step1()
			} else {
				fmt.Println("取消注册")
				os.Exit(1)
			}
		}
		fmt.Println("注册完成，请使用daemon启动")
	},
}

func getNewMinerID() (uint64, int) {
	fmt.Println("获取ID")
	rand.Seed(time.Now().Unix())
	randInt := rand.Int()
	fmt.Println(BPList)
	bpIndex := randInt % len(BPList)

	fmt.Println(randInt, bpIndex, len(BPList))
	currBP := BPList[bpIndex]
	requestUrl := fmt.Sprintf("http://%s:8082/newnodeid", currBP)

	resp, err := http.Get(requestUrl)
	if err != nil {
		fmt.Println("申请账号失败！", err)
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("申请账号失败！", err)
	}
	var resData struct {
		NodeID uint64 `json:"nodeid"`
	}
	err = json.Unmarshal(buf, &resData)
	if err != nil {
		fmt.Println("申请账号失败！", err)
	}
	return resData.NodeID, bpIndex
}

func newCfg() (*config.Config, error) {
	var GB uint64 = 1 << 30
	var size uint64 = 4096
	var mc byte = 14
	fmt.Println("请输入矿机存储空间大小GB例如4T=4096:")
	_, err := fmt.Scanf("%d\n", &size)
	if err != nil {
		log.Println(err)
	}
	mc = 20
	commander.InitBySignleStorage(size*GB, 1<<mc)
	_cfg, err := config.ReadConfig()
	if err != nil {
		return nil, err
	}
	return _cfg, nil
}

func readCfg() (*config.Config, error) {
	_cfg, err := config.ReadConfig()
	if err != nil {
		return _cfg, err
	}
	adminacc = _cfg.Adminacc
	minerid = uint64(_cfg.IndexID)
	return _cfg, nil
}

func step1() {

	type minerData struct {
		MinerID   uint64          `json:"minerid"`
		AdminAcc  eos.AccountName `json:"adminacc"`
		DepAcc    eos.AccountName `json:"dep_acc"`
		DepAmount eos.Asset       `json:"dep_amount"`
		Extra     string          `json:"extra"`
	}
	initConfig, err := newCfg()

	if err != nil {
		fmt.Println("初始化错误:", err)
		os.Exit(1)
	}

	minerid, bi = getNewMinerID()
	initConfig.IndexID = uint32(minerid)

	// fmt.Println("请输入抵押账号用户名：")
	// fmt.Scanf("%s\n", &depAcc)
	// fmt.Println("请输入矿机管理员账号：")
	// fmt.Scanf("%s\n", &adminacc)
	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("newminer"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(depAcc), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(minerData{
			minerid,
			eos.AN("admin"),
			eos.AN("depAcc"),
			newYTAAssect(depAmount),
			initConfig.PubKey,
		}),
	}

	txOpts := &eos.TxOptions{}
	txOpts.FillFromChain(api)
	tx := eos.NewSignedTransaction(eos.NewTransaction([]*eos.Action{action}, txOpts))
	tx.SetExpiration(time.Minute * 30)

post:
	fmt.Println("注册信息：")
	fmt.Println("矿工ID：", minerid)
	fmt.Println("管理账号用户名：", adminacc)
	fmt.Println("抵押账号用户名：", depAcc)
	fmt.Println("抵押额度：", depAmount)
	fmt.Println("是否开始注册 y/n?")
	fmt.Scanf("%c\n", &yOrN)

	if yOrN == 'n' {
		fmt.Println("取消注册")
		jsonbuf, _ := json.Marshal(tx)
		fmt.Println(string(jsonbuf))
		os.Exit(1)
	}
	err = preRegister(tx)
	if err != nil {
		fmt.Println(err)
		goto post
	}
	initConfig.Adminacc = adminacc
	initConfig.Save()
}
func preRegister(tx *eos.SignedTransaction) error {
	packedtx, err := tx.Pack(eos.CompressionZlib)

	if err != nil {
		fmt.Println(err)
		return err
	}
	//out, err := api.PushTransaction(packedtx)
	//if err != nil {
	//	return err
	//}
	//log.Println(out.StatusCode, out.BlockID)
	//return nil
	buf, err := json.Marshal(packedtx)
	if err != nil {
		log.Println(err)
	}
	resp, err := http.Post(fmt.Sprintf("http://%s:8082/preregnode", BPList[bi]), "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		res, err := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("%s,%s,%v", resp.Status, res, err)
	}

	//log.Println(string(buf))
	return nil
}

func getNodeList() []string {

	var list []string
	for i := 0; i < 21; i++ {
		if i < 10 {
			list = append(list, fmt.Sprintf("sn0%d.yottachain.net", i))
		} else {
			list = append(list, fmt.Sprintf("sn%d.yottachain.net", i))
		}
	}
	return list
}

func newYTAAssect(amount int64) eos.Asset {
	var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTA"}
	return eos.Asset{Amount: eos.Int64(amount) * eos.Int64(math.Pow(10, float64(YTASymbol.Precision))), Symbol: YTASymbol}
}

func getPubkey() []ecc.PublicKey {
	var pkeys = make([]ecc.PublicKey, len(kb.Keys))
	for k, v := range kb.Keys {
		pkeys[k] = v.PublicKey()
	}
	return pkeys
}
