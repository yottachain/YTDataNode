package registerCmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"path"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"
	"gopkg.in/yaml.v2"

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
var formPath string

var kb = eos.NewKeyBag()
var maxSpace uint64 = 268435456

var PoolAdminKey string
var DepAccKey string

var RegisterCmd = &cobra.Command{
	Short: "注册账号",
	Use:   "register",
	Run: func(cmd *cobra.Command, args []string) {
		initConfig, err := readCfg()
		if err == nil && initConfig.IndexID != 0 {
			fmt.Println("矿机已经注册，若要重新注册请备份或删除旧矿机")
			return
		}
		var form RegForm
		if formPath != "" {
			file, err := os.OpenFile(formPath, os.O_RDONLY, 0644)
			if err != nil {
				log.Println(err)
				return
			}
			defer file.Close()
			dd := yaml.NewDecoder(file)
			err = dd.Decode(&form)
			if err != nil {
				log.Println(err)
				return
			}
		}

		step1(&form)
	},
}

func init() {
	p, _ := os.Executable()
	RegisterCmd.Flags().StringVar(
		&formPath, "form",
		path.Join(p, "form.yaml"),
		"注册提交表单的路径",
	)
	RegisterCmd.Flags().StringVar(
		&PoolAdminKey, "pk",
		"",
		"矿池管理员私钥",
	)
	RegisterCmd.Flags().StringVar(
		&DepAccKey, "dk",
		"",
		"抵押账户私钥",
	)
	err := kb.ImportPrivateKey(PoolAdminKey)
	if err != nil {
		fmt.Println("矿池私钥输入错误:", err.Error())
	}
	err = kb.ImportPrivateKey(DepAccKey)
	if err != nil {
		fmt.Println("抵押私钥输入错误:", err.Error())
	}

	api = eos.New(baseNodeUrl)
	api.SetSigner(kb)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		return getPubkey(), nil
	})
}

// 获取一个随机BP
func getRandBPUrl(BPList []string) string {
	rand.Seed(time.Now().Unix())
	randInt := rand.Int()
	fmt.Println(BPList)
	bpIndex := randInt % len(BPList)

	fmt.Println(randInt, bpIndex, len(BPList))
	return BPList[bpIndex]
}

// 获取一个新的矿机ID
// @params requestUrl string 请求的BP的URL
// @return minerId uint64 返回可用的矿机ID
func getNewMinerID(requestUrl string) uint64 {

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
	return resData.NodeID
}

func newCfg(form *RegForm) (*config.Config, error) {
	var GB uint64 = 1 << 30
	commander.InitBySignleStorage(form.MaxSpace*GB, 2048)

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
	return _cfg, nil
}

func step1(form *RegForm) {
	if form.BaseUrl != "" {
		baseNodeUrl = ""
	}

	type minerData struct {
		MinerID    uint64          `json:"minerid"`
		AdminAcc   eos.AccountName `json:"adminacc"`
		DepAcc     eos.AccountName `json:"dep_acc"`
		DepAmount  eos.Asset       `json:"dep_amount"`
		PoolID     eos.AccountName `json:"pool_id"`
		MinerOwner eos.AccountName `json:"minerowner"`
		MaxSpace   uint64          `json:"max_space"`
		IsCalc     bool            `json:"is_calc"`
		Extra      string          `json:"extra"`
	}
	initConfig, err := newCfg(form)

	if err != nil {
		fmt.Println("初始化错误:", err)
		os.Exit(1)
	}

	currBP := getRandBPUrl(form.BPList)
	minerid := getNewMinerID(GetNewMinerIDUrl(currBP))
	initConfig.IndexID = uint32(minerid)

	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("newminer"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(form.DepAcc), Permission: eos.PN("active")},
			{Actor: eos.AN(form.PoolAdmin), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(minerData{
			MinerID:    minerid,
			AdminAcc:   eos.AN(form.AdminAcc),
			DepAcc:     eos.AN(form.DepAcc),
			DepAmount:  newYTAAssect(int64(form.DepAmount)),
			IsCalc:     form.IsCalc,
			PoolID:     eos.AN(form.AdminAcc),
			MinerOwner: eos.AN(form.MinerOwner),
			MaxSpace:   form.MaxSpace / 16384,
			Extra:      initConfig.PubKey,
		}),
	}

	txOpts := &eos.TxOptions{}
	txOpts.FillFromChain(api)
	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	tx.SetExpiration(time.Minute * 30)
	sigedTx, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionZlib)

	if err != nil {
		fmt.Println("注册失败！", err)
		return
	}

	err = Register(sigedTx, packedTx, currBP)
	if err != nil {
		fmt.Println(err)
		return
	}
	initConfig.Adminacc = form.AdminAcc
	initConfig.PoolID = form.PoolId
	initConfig.Save()
}
func Register(tx *eos.SignedTransaction, packedtx *eos.PackedTransaction, bpUrl string) error {

	buf, err := json.Marshal(packedtx)
	if err != nil {
		log.Println(err)
	}
	resp, err := http.Post(GetRegisterUrl(bpUrl), "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		res, err := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("%s,%s,%v", resp.Status, res, err)
	}
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
