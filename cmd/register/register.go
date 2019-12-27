package registerCmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/yottachain/YTDataNode/logger"
	"math"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"

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
var depAmount int64
var key1 string
var key2 string = "5JkjKo4UGaTQFVuVpDZDV3LNvLrd2DgGRpTNB4E1o9gVuUf7aYZ"
var kb = eos.NewKeyBag()
var maxSpace uint64 = 268435456

//var initConfig config.Config

var yOrN byte

func init() {
	// resp, err := http.Get("http://download.yottachain.io/config/bpbaseurl")
	// if err != nil {
	// 	fmt.Println("获取BP入口失败")
	// 	os.Exit(1)
	// }
	// buf, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	fmt.Println("获取BP入口失败")
	// 	os.Exit(1)
	// }
	// baseNodeUrl = strings.Replace(string(buf), "\n", "", -1)
}

type PoolInfo []struct {
	PoolID    string `json:"pool_id"`
	PoolOwner string `json:"pool_owner"`
	MaxSpace  uint64 `json:"max_space"`
}

func getPoolInfo(poolID string) (PoolInfo, error) {
	out, err := api.GetTableRows(eos.GetTableRowsRequest{
		Code:       "hddpool12345",
		Scope:      "hddpool12345",
		Table:      "storepool",
		Index:      "1",
		Limit:      1,
		LowerBound: poolID,
		UpperBound: poolID,
		JSON:       true,
		KeyType:    "name",
	})
	if err != nil {
		return nil, err
	}
	var res PoolInfo
	json.Unmarshal(out.Rows, &res)
	return res, nil
}

var RegisterCmd = &cobra.Command{
	Short: "注册账号",
	Use:   "register",
	Run: func(cmd *cobra.Command, args []string) {
		//var poolID string
		BPList = getNodeList()
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
		} else {
			if initConfig.PoolID != "" {
				fmt.Printf("已加入%s矿池，是否需要加入新的矿池", initConfig.PoolID)
				fmt.Scanf("%c\n", yOrN)
				if yOrN == 'n' {
					fmt.Println("取消加入新矿池")
					os.Exit(1)
				}
			}
		}
		step2()
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
getMC:
	fmt.Println("请输入存储分组数:可能取值8～20间")
	_, err = fmt.Scanf("%d\n", &mc)
	if err != nil {
		log.Println(err)
		goto getMC
	}
	if mc < 8 || mc > 20 {
		fmt.Println("请输入范围8～20的数", mc)
		goto getMC
	}
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

	var regTxsigned string

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

	fmt.Println("请输入抵押账号用户名：")
	fmt.Scanf("%s\n", &depAcc)
	//log.Println("请输入抵押账号私钥：")
	//fmt.Scanf("%s\n", &key1)
	fmt.Println("请输入抵押额度(YTA)：")

	fmt.Scanf("%d\n", &depAmount)
	fmt.Println("请输入矿机管理员账号：")
	fmt.Scanf("%s\n", &adminacc)
	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("newminer"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(depAcc), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(minerData{
			minerid,
			eos.AN(adminacc),
			eos.AN(depAcc),
			newYTAAssect(depAmount),
			initConfig.PubKey,
		}),
	}

	txOpts := &eos.TxOptions{}
	txOpts.FillFromChain(api)
	tx := eos.NewSignedTransaction(eos.NewTransaction([]*eos.Action{action}, txOpts))
	tx.SetExpiration(time.Minute * 30)

regTxsign:
	fmt.Println("请对如下交易进行签名并粘贴:")
	txjson, err := json.Marshal(tx)
	fmt.Printf("%s\n", txjson)
	fmt.Println("-----------------------------")
	fmt.Scanln(&regTxsigned)
	json.Unmarshal([]byte(regTxsigned), &tx)
	if err != nil {
		fmt.Println("签名错误：", err)
		goto regTxsign
	}

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

func step2() {
	var poolID string
	var txSigned string
	var minerOwner string

	fmt.Println("是否加入矿池:y/n?")
	fmt.Scanf("%c\n", &yOrN)
	if yOrN == 'n' {
		fmt.Println("取消加入矿池")
		os.Exit(1)
	}
	type Data struct {
		MinerID    uint64          `json:"miner_id"`
		PoolID     eos.AccountName `json:"pool_id"`
		Minerowner eos.AccountName `json:"minerowner"`
		MaxSpace   uint64          `json:"max_space"`
	}
getPoolInfo:
	log.Println("请输入矿池id")
	fmt.Scanf("%s\n", &poolID)
	log.Println("请输入配额")
	fmt.Scanf("%d\n", &maxSpace)
	log.Println("请输入收益账号")
	fmt.Scanf("%s\n", &minerOwner)
	//log.Println("请输入配额（单位：block）")
	//fmt.Scanf("%d\n", &maxSpace)
	pi, err := getPoolInfo(poolID)
	if err != nil || len(pi) == 0 {
		fmt.Println("获取矿池信息失败！", pi, err)
		goto getPoolInfo
	}
	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("addm2pool"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(adminacc), Permission: eos.PN("active")},
			{Actor: eos.AN(pi[0].PoolOwner), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(Data{
			MinerID:    minerid,
			Minerowner: eos.AN(minerOwner),
			PoolID:     eos.AN(pi[0].PoolID),
			MaxSpace:   maxSpace,
		}),
	}
	txOpts := &eos.TxOptions{}
	txOpts.FillFromChain(api)
	tx := eos.NewSignedTransaction(eos.NewTransaction([]*eos.Action{action}, txOpts))
	tx.SetExpiration(time.Minute * 30)
	log.Printf("交易信息 %v", action)
addPoolSign:
	fmt.Println("请对交易进行签名并粘贴：")
	fmt.Println("----------------------")
	txjson, err := json.Marshal(tx)
	fmt.Printf("%s\n", txjson)
	txSigned = util.ReadStringLine(os.Stdin, 4096)
	json.Unmarshal([]byte(txSigned), &tx)
	if err != nil {
		fmt.Println("签名错误：", err)
		goto addPoolSign
	}
	err = addPool(tx)
	if err != nil {
		fmt.Println("加入矿池失败：", err)
		goto addPoolSign
	}
	initConfig, err := readCfg()
	if err != nil {
		log.Println("读取配置失败")
	}
	initConfig.PoolID = poolID
	initConfig.Save()
}

func addPool(tx *eos.SignedTransaction) error {
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
	resp, err := http.Post(fmt.Sprintf("http://%s:8082/changeminerpool", BPList[bi]), "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf(resp.Status)
	}

	return nil
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
		return fmt.Errorf(resp.Status)
	}
	//log.Println(string(buf))
	return nil
}

func getNodeList() []string {
	var list []string
	//resp, err := http.Get("http://download.yottachain.io/config/bpsn-test.json")
	//if err != nil {
	//	log.Println("获取BP失败")
	//	os.Exit(1)
	//}
	//buf, err := ioutil.ReadAll(resp.Body)
	//if err != nil {
	//	log.Println("获取BP失败")
	//	os.Exit(1)
	//}
	//listStr := `
	//["49.234.139.206",
	//"129.211.72.15",
	//"122.152.203.189",
	//"212.129.153.253",
	//"49.235.52.30"]
	//`
	//buf := bytes.NewBufferString(listStr)
	for i := 0; i < 21; i++ {
		if i < 10 {
			list = append(list, fmt.Sprintf("sn0%d.yottachain.net", i))
		} else {
			list = append(list, fmt.Sprintf("sn%d.yottachain.net", i))
		}
	}
	//err = json.Unmarshal(buf, &list)
	//if err != nil {
	//	log.Println(err)
	//}
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
