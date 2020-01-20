package register_api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go"
	"github.com/yottachain/YTDataNode/commander"
	"io/ioutil"
	"math"
	"net/http"
)

// 初始化仓库
func InitRepo(size uint64, n uint32) error {
	return commander.InitBySignleStorage(size, n)
}

type MinerData struct {
	MinerID   uint64          `json:"minerid"`
	AdminAcc  eos.AccountName `json:"adminacc"`
	DepAcc    eos.AccountName `json:"dep_acc"`
	DepAmount eos.Asset       `json:"dep_amount"`
	Extra     string          `json:"extra"`
}
type ADDPoolData struct {
	MinerID    uint64          `json:"miner_id"`
	PoolID     eos.AccountName `json:"pool_id"`
	Minerowner eos.AccountName `json:"minerowner"`
	MaxSpace   uint64          `json:"max_space"`
}

func GetSNList(baseaddr string) ([]string, error) {
	var list []string
	resp, err := http.Get(baseaddr)
	if err != nil {
		return nil, err
	}
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(buf, &list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

type API struct {
	*eos.API
	SNAddr string
}

// GetNewMinerID 获取新的矿机id
func (api *API) GetNewMinerID() (uint64, error) {
	url := fmt.Sprintf("http://%s:8082/newnodeid", api.SNAddr)
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var resData struct {
		NodeID uint64 `json:"nodeid"`
	}
	err = json.Unmarshal(buf, &resData)
	if err != nil {
		return 0, err
	}
	return resData.NodeID, nil
}

func (api *API) GetPoolInfo() {

}

// NewRegisterTransaction 创建矿机注册事务
func (api *API) NewRegisterTransaction(data MinerData) *eos.Transaction {
	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("newminer"),
		Authorization: []eos.PermissionLevel{
			{Actor: data.DepAcc, Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(data),
	}
	txOpt := eos.TxOptions{}
	txOpt.FillFromChain(api.API)
	tx := eos.NewTransaction([]*eos.Action{action}, &txOpt)

	eos.
	return tx
}

// PushTransactionToSN 向超级节点推送事务
func (api *API) PushTransactionToSN(tx *eos.SignedTransaction, url string) error {
	toUrl := fmt.Sprintf("http://%s%s", api.SNAddr, url)
	packedtx, err := tx.Pack(eos.CompressionZlib)
	if err != nil {
		return err
	}
	buf, err := json.Marshal(packedtx)
	if err != nil {
		return err
	}
	resp, err := http.Post(toUrl, "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf(resp.Status)
	}
	return nil
}

func NewYTAAssect(amount int64) eos.Asset {
	var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTA"}
	return eos.Asset{Amount: eos.Int64(amount) * eos.Int64(math.Pow(10, float64(YTASymbol.Precision))), Symbol: YTASymbol}
}
