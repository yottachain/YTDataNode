package registerCmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"
	"gopkg.in/yaml.v2"

	"github.com/spf13/cobra"
)

const GB = 1 << 30

var baseNodeUrl = "http://dnapi1.yottachain.net:8888" //正式

var api *eos.API
var formPath string

var kb = eos.NewKeyBag()

var RegisterCmd = &cobra.Command{
	Short: "注册账号",
	Use:   "register",
	Run: func(cmd *cobra.Command, args []string) {
		initConfig, err := readCfg()
		if err == nil && initConfig.IndexID != 0 {
			fmt.Println("矿机已经注册，若要重新注册请备份或删除旧矿机(ytfs_path下相关文件)")
			return
		}
		var form RegForm
		if formPath != "" {
			file, err := os.OpenFile(formPath, os.O_RDONLY, 0644)
			if err != nil {
				log.Printf("the register form file open err:%s", err)
				return
			}
			defer file.Close()
			dd := yaml.NewDecoder(file)
			err = dd.Decode(&form)
			if err != nil {
				log.Printf("the register form file err:%s", err)
				return
			}
		} else {
			log.Printf("the register form file does not exist")
			return
		}

		if form.BaseUrl != "" {
			baseNodeUrl = form.BaseUrl
		}

		err = kb.ImportPrivateKey(form.PoolOwnerKey)
		if err != nil {
			fmt.Println("矿池私钥输入错误:", err.Error())
			return
		}
		if form.PoolOwnerKey != form.DepAccKey {
			err = kb.ImportPrivateKey(form.DepAccKey)
			if err != nil {
				fmt.Println("抵押私钥输入错误:", err.Error())
				return
			}
		}

		api = eos.New(baseNodeUrl)
		api.SetSigner(kb)
		api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
			return getPubkey(), nil
		})

		step1(&form)
	},
}

type PoolInfo []struct {
	PoolID    string `json:"pool_id"`
	PoolOwner string `json:"pool_owner"`
	MaxSpace  uint64 `json:"max_space"`
}

func init() {
	p, _ := os.Executable()
	RegisterCmd.Flags().StringVar(
		&formPath, "form",
		path.Join(p, "form.yaml"),
		"注册提交表单的路径",
	)
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
func getNewMinerID(requestUrl string) (uint64, error) {

	resp, err := http.Get(requestUrl)
	if err != nil {
		fmt.Println("申请账号失败！", err)
		return 0, err
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("申请账号失败！", err)
		return 0, err
	}
	var resData struct {
		NodeID uint64 `json:"nodeid"`
	}
	err = json.Unmarshal(buf, &resData)
	if err != nil {
		fmt.Println("申请账号失败！", err)
		return 0, err
	}
	return resData.NodeID, nil
}

func newCfg(form *RegForm) (*config.Config, error) {
	var GB uint64 = 1 << 30
	var M uint32
	if form.M <= 0 || form.M > 2048 {
		M = 2048
	} else {
		M = form.M
	}

	cfg := commander.InitBySignleStorage(form.MaxSpace*GB, M, form.ISBlockDev, form.StoragePath)
	if cfg == nil {
		return nil, fmt.Errorf("new config error cfg is nil")
	}

	_ = cfg.Save()

	return cfg, nil
}

func readCfg() (*config.Config, error) {
	_cfg, err := config.ReadConfig()
	if err != nil {
		return _cfg, err
	}
	return _cfg, nil
}

func step1(form *RegForm) {
	type minerData struct {
		MinerID    uint64          `json:"minerid"`
		AdminAcc   eos.AccountName `json:"adminacc"`
		DepAcc     eos.AccountName `json:"dep_acc"`
		PoolID     eos.AccountName `json:"pool_id"`
		MinerOwner eos.AccountName `json:"minerowner"`
		MaxSpace   uint64          `json:"max_space"`
		DepAmount  eos.Asset       `json:"dep_amount"`
		IsCalc     bool            `json:"is_calc"`
		Extra      string          `json:"extra"`
	}
	initConfig, err := newCfg(form)

	if err != nil {
		fmt.Println("初始化错误:", err)
		os.Exit(1)
	}

	currBP := getRandBPUrl(form.BPList)
	minerid, err := getNewMinerID(GetNewMinerIDUrl(currBP))
	if err != nil {
		os.Exit(1)
	}
	initConfig.IndexID = uint32(minerid)

	actionData := minerData{
		MinerID:    minerid,
		AdminAcc:   eos.AN(form.AdminAcc),
		DepAcc:     eos.AN(form.DepAcc),
		DepAmount:  newYTAAssect(int64(form.DepAmount)),
		IsCalc:     form.IsCalc,
		PoolID:     eos.AN(form.PoolId),
		MinerOwner: eos.AN(form.MinerOwner),
		//MaxSpace:   form.MaxSpace * GB / (config.GlobalShardSize * 1024),
		MaxSpace: form.MaxSpace * GB / (16 * 1024),
		Extra:    initConfig.PubKey,
	}
	action := &eos.Action{
		Account:       eos.AN("hddpool12345"),
		Name:          eos.ActN("regminer"),
		Authorization: []eos.PermissionLevel{},
		ActionData:    eos.NewActionData(actionData),
	}

	action.Authorization = append(
		action.Authorization,
		eos.PermissionLevel{Actor: eos.AN(form.DepAcc), Permission: eos.PN("active")},
	)
	if form.PoolOwnerKey != form.DepAccKey {
		action.Authorization = append(
			action.Authorization,
			eos.PermissionLevel{Actor: eos.AN(form.PoolOwner), Permission: eos.PN("active")},
		)
	}

	txOpts := &eos.TxOptions{}
	txOpts.FillFromChain(api)
	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	tx.SetExpiration(time.Minute * 30)
	sigedTx, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionZlib)

	if err != nil {
		fmt.Printf("register fail！ err:%s\n", err.Error())
		return
	}
	fmt.Println("registering... ",
		"poolID", actionData.PoolID,
		"minerID", actionData.MinerID,
		"depAmount", actionData.DepAmount.Amount,
		"maxSpace", actionData.MaxSpace,
		"currBP", currBP)
	ShardSize, err := Register(sigedTx, packedTx, currBP)
	if err != nil {
		fmt.Printf("register fail! err:%s\n", err.Error())
		return
	} else {
		fmt.Printf("miner register success!, shard size %d\n", ShardSize)
	}

	initConfig.ShardSize = ShardSize
	initConfig.Adminacc = form.AdminAcc
	initConfig.PoolID = form.PoolId
	initConfig.Save()
}

func Register(tx *eos.SignedTransaction, packedtx *eos.PackedTransaction, bpUrl string) (int32, error) {
	var ShardSize = int32(0)
	buf, err := json.Marshal(packedtx)
	if err != nil {
		log.Println(err)
	}
	resp, err := http.Post(GetRegisterUrl(bpUrl), "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		res, err := ioutil.ReadAll(resp.Body)
		return 0, fmt.Errorf("%s,%s,%v", resp.Status, res, err)
	} else {
		var resData struct {
			ShardSize int32 `json:"shardsize"`
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, fmt.Errorf("get shard size error %v", err)
		}
		err = json.Unmarshal(body, &resData)
		if err != nil {
			return 0, fmt.Errorf("get shard size error %v", err)
		}
		ShardSize = resData.ShardSize
	}
	return ShardSize, nil
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
