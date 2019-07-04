package registerCmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"
	"log"
	"math"

	//"github.com/eoscanada/eos-go/ecc"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var baseNodeUrl = "http://35.176.59.89:8888"

var api = eos.New(baseNodeUrl)
var BPList []string
var bpindex int
var minerid uint64
var adminacc string
var depAcc string
var depAmount int64
var key1 string
var key2 string = "5JkjKo4UGaTQFVuVpDZDV3LNvLrd2DgGRpTNB4E1o9gVuUf7aYZ"
var kb = eos.NewKeyBag()
var cfg config.Config

var yOrN byte

func init() {
	resp, err := http.Get("http://download.yottachain.io/config/bpbaseurl")
	if err != nil {
		fmt.Println("获取BP入口失败")
		os.Exit(1)
	}
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("获取BP入口失败")
		os.Exit(1)
	}
	baseNodeUrl = strings.Replace(string(buf), "\n", "", -1)
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
		if err := readCfg(); err != nil {
			fmt.Println("未查询到矿机注册信息，是否重新注册：y/n?")
			fmt.Scanf("%c\n", yOrN)
			if yOrN != 'n' {
				step1()
			} else {
				fmt.Println("取消注册")
				os.Exit(1)
			}
			if cfg.PoolID != "" {
				fmt.Printf("已加入%s矿池，是否需要加入新的矿池", cfg.PoolID)
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
	rand.Seed(time.Now().Unix())
	bpIndex := rand.Int() % len(BPList)
	currBP := BPList[bpIndex]
	bpurl, err := url.Parse(currBP)
	requestUrl := fmt.Sprintf("http://%s:8082/newnodeid", bpurl.Host)
	if err != nil {
		fmt.Println("申请账号失败！", err)
	}
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
	fmt.Println(resData.NodeID, bpindex, BPList)
	return resData.NodeID, bpIndex
}

func newCfg() (*config.Config, error) {
	commander.Init()
	cfg, err := config.ReadConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func readCfg() error {
	cfg, err := config.ReadConfig()
	if err != nil {
		return err
	}
	adminacc = cfg.Adminacc
	minerid = uint64(cfg.IndexID)
	return nil
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
	cfg, err := newCfg()

	if err != nil {
		fmt.Println("初始化错误:", err)
		os.Exit(1)
	}

	minerid, bpindex = getNewMinerID()
	cfg.IndexID = uint32(minerid)
	cfg.Save()

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
			cfg.PubKey,
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
	fmt.Scanf("%s\n", &regTxsigned)
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
	cfg.Adminacc = adminacc
	cfg.Save()
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
	log.Println("请输入收益账号")
	fmt.Scanf("%s\n", &minerOwner)
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
			MaxSpace:   268435456,
		}),
	}
	txOpts := &eos.TxOptions{}
	txOpts.FillFromChain(api)
	tx := eos.NewSignedTransaction(eos.NewTransaction([]*eos.Action{action}, txOpts))
	tx.SetExpiration(time.Minute * 30)
addPoolSign:
	fmt.Println("请对交易进行签名并粘贴：")
	fmt.Println("----------------------")
	txjson, err := json.Marshal(tx)
	fmt.Printf("%s\n", txjson)
	fmt.Scanf("%s\n", &txSigned)
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
	cfg.PoolID = poolID
	cfg.Save()
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
	resp, err := http.Post(fmt.Sprintf("%s:8082/changeminerpool", BPList[bpindex]), "applaction/json", bytes.NewBuffer(buf))
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
	resp, err := http.Post(fmt.Sprintf("%s:8082/preregnode", BPList[bpindex]), "applaction/json", bytes.NewBuffer(buf))
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
	url := fmt.Sprintf("%s/v1/chain/get_producers", baseNodeUrl)
	type Params struct {
		Json bool `json:"json"`
	}
	type ResponseSchem struct {
		Rows []struct {
			URL   string `json:"url"`
			Owner string `json:"owner"`
		} `json:"rows"`
	}
	p := Params{
		true,
	}
	buf, _ := json.Marshal(p)
	resp, err := http.Post(url, "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		fmt.Println("获取节点列表失败")
		fmt.Println(err)
		os.Exit(1)
	}
	resData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("获取节点列表失败")
		fmt.Println(err)
		os.Exit(1)
	}
	var res ResponseSchem
	json.Unmarshal(resData, &res)
	defer resp.Body.Close()
	inServiceNode := getInServiceNode()
	var list = make([]string, len(inServiceNode))
	for k, v := range res.Rows {
		for _, v2 := range inServiceNode {
			if v2 == v.Owner {
				list[k] = strings.Replace(v.URL, ":8888", "", -1)
			}
		}
	}
	return list
}

func getInServiceNode() []string {
	url := fmt.Sprintf("%s/v1/chain/get_producer_schedule", baseNodeUrl)
	type Params struct {
		Json bool `json:"json"`
	}
	type ResponseSchem struct {
		Active struct {
			Producers []struct {
				ProducerName string `json:"producer_name"`
			} `json:"producers"`
		} `json:'active'`
	}
	p := Params{
		true,
	}
	buf, _ := json.Marshal(p)
	resp, err := http.Post(url, "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		fmt.Println("获取节点列表失败")
		fmt.Println(err)
		os.Exit(1)
	}
	resData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("获取节点列表失败")
		fmt.Println(err)
		os.Exit(1)
	}
	var res ResponseSchem
	json.Unmarshal(resData, &res)
	defer resp.Body.Close()
	var list = make([]string, len(res.Active.Producers))
	for k, v := range res.Active.Producers {
		list[k] = v.ProducerName
	}
	return list
}

func newYTAAssect(amount int64) eos.Asset {
	var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTT"}

	return eos.Asset{Amount: eos.Int64(amount) * eos.Int64(math.Pow(10, float64(YTASymbol.Precision))), Symbol: YTASymbol}
}

func getPubkey() []ecc.PublicKey {
	var pkeys = make([]ecc.PublicKey, len(kb.Keys))
	for k, v := range kb.Keys {
		pkeys[k] = v.PublicKey()
	}
	return pkeys
}
