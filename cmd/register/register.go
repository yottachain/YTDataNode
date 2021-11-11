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

	//"github.com/yottachain/YTDataNode/util"
	comm "github.com/yottachain/YTFS/common"

	"gopkg.in/yaml.v2"


	//"github.com/eoscanada/eos-go/ecc"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

const GB = 1 << 30

var baseNodeUrl = "http://dnapi1.yottachain.net:8888" //正式
// var baseNodeUrl = "http://117.161.159.11:8888" //测试

var api *eos.API
var formPath string

var kb = eos.NewKeyBag()

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

		if form.BaseUrl != "" {
			baseNodeUrl = form.BaseUrl
		}

		err = kb.ImportPrivateKey(form.PoolOwnerKey)
		if err != nil {
			fmt.Println("矿池私钥输入错误:", err.Error())
		}
		if form.PoolOwnerKey != form.DepAccKey {
			err = kb.ImportPrivateKey(form.DepAccKey)
			if err != nil {
				fmt.Println("抵押私钥输入错误:", err.Error())
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

	var size uint64 = 4096
	var mc byte = 14
	var db string = "rocksdb"
	var stortype uint32 = 0;
	var devname = "storage"

	fmt.Println("请输入矿机存储空间大小GB例如4T=4096:")
	_, err := fmt.Scanf("%d\n", &size)
	if err != nil {
		log.Println(err)
	}
getMC:
	fmt.Println("请输入选择使用的数据库: rocksdb | indexdb")
	_, err = fmt.Scanf("%s\n", &db)
	if err != nil {
		log.Println(err)
		goto getMC
	}

	if db != "rocksdb" && db != "indexdb"{
		fmt.Println("数据库选择错误")
		goto getMC
	}

	if "indexdb" == db {
		fmt.Println("请输入存储分组数:可能取值14～20间")
		_, err = fmt.Scanf("%d\n", &mc)
		if err != nil {
			log.Println(err)
			goto getMC
		}
		if mc < 14 || mc > 20 {
			fmt.Println("请输入范围14～20的数", mc)
			goto getMC
		}
	}

	fmt.Println("请输入后端存储设备类型: 0(文件)| 1(块设备)")
	_, err = fmt.Scanf("%d\n", &stortype)
	if err != nil {
		log.Println(err)
		goto getMC
	}

	if 0 == stortype {
		fmt.Println("请输入后端文件名称: storage(默认)")
		_, err = fmt.Scanf("%s\n", &devname)
		if err != nil {
			log.Println(err)
			goto getMC
		}
	}else if 1 == stortype {
		fmt.Println("请输入后端块设备名称: ")
		_, err = fmt.Scanf("%s\n", &devname)
		if err != nil {
		    log.Println(err)
		    goto getMC
	    }
	} else {
		fmt.Println("后端存储类型设置错误")
		goto getMC
	}

	stype := comm.StorageType(stortype)
	err = commander.InitBySignleStorage(size*GB, 1<<mc, db, stype, devname)
	if err != nil{
		fmt.Println("init storage error!")
		return nil, err
	}

	//commander.InitBySignleStorage(form.MaxSpace*GB, 2048)


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
	minerid := getNewMinerID(GetNewMinerIDUrl(currBP))
	initConfig.IndexID = uint32(minerid)

	actionData := minerData{
		MinerID:    minerid,
		AdminAcc:   eos.AN(form.AdminAcc),
		DepAcc:     eos.AN(form.DepAcc),
		DepAmount:  newYTAAssect(int64(form.DepAmount)),
		IsCalc:     form.IsCalc,
		PoolID:     eos.AN(form.PoolId),
		MinerOwner: eos.AN(form.MinerOwner),
		MaxSpace:   form.MaxSpace * GB / 16384,
		Extra:      initConfig.PubKey,
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
		fmt.Println("注册失败！", err)
		return
	}
	fmt.Println("正在注册")
	fmt.Println("poolID", actionData.PoolID, "minerID", actionData.MinerID, "depAmount", actionData.DepAmount.Amount, "maxSpace", actionData.MaxSpace)
	fmt.Println("currBP", currBP)
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
	log.Println("注册完成")
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
