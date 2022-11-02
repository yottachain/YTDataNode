package transaction

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/yottachain/YTDataNode/config"
)

var Api *eos.API

// 新增的通用api
func newYTAAssect(amount int64) eos.Asset {
	var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTA"}
	return eos.Asset{Amount: eos.Int64(amount) * eos.Int64(math.Pow(10, float64(YTASymbol.Precision))), Symbol: YTASymbol}
}
func NewYTAAssect(amount int64) eos.Asset {
	return newYTAAssect(amount)
}

func getPubkey(kb *eos.KeyBag) []ecc.PublicKey {
	var pkeys = make([]ecc.PublicKey, len(kb.Keys))
	for k, v := range kb.Keys {
		pkeys[k] = v.PublicKey()
	}
	return pkeys
}
func GetPubKey(kb *eos.KeyBag) []ecc.PublicKey {
	return getPubkey(kb)
}

func GetActionData(ad interface{}) (*eos.ActionData, error) {
	if reflect.ValueOf(ad).IsNil() {
		return nil, fmt.Errorf("ad must is address")
	}
	t := reflect.TypeOf(ad).Elem()
	v := reflect.ValueOf(ad).Elem()
	for i := 0; i < t.NumField(); i++ {
		tags := t.Field(i).Tag
		prompt := tags.Get("prompt")
		required := tags.Get("required")
		convert := tags.Get("convert")
	input:
		if prompt != "" {
			fmt.Printf("%s\n", prompt)
		} else {
			continue
		}
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		text := scanner.Text()
		if required == "true" && text == "" {
			fmt.Println("该字段不能为空")
			goto input
		}
		typename := t.Field(i).Type.Name()
		if convert != "" {
			typename = convert
		}
		switch typename {
		case "string", "AccountName", "ActionName":
			var value string
			fmt.Sscanln(text, &value)
			v.Field(i).SetString(value)
		case "uint", "uint8", "uint16", "uint32", "uint64":
			var value uint64
			fmt.Sscanln(text, &value)
			v.Field(i).SetUint(value)
		case "int", "int8", "int16", "int32", "int64":

			var value int64
			fmt.Sscanln(text, &value)
			v.Field(i).SetInt(value)
		case "float", "float32", "float64":
			var value float64
			fmt.Sscanln(text, &value)
			v.Field(i).SetFloat(value)
		case "bool":
			var value string
			fmt.Sscanln(text, &value)
			switch value {
			case "yes", "y", "true":
				v.Field(i).SetBool(true)
			case "no", "n", "false":
				v.Field(i).SetBool(false)
			default:
				fmt.Println("输入错误")
				goto input
			}
		case "Asset":
			var value int64
			fmt.Sscanln(text, &value)
			ast := newYTAAssect(value)
			v.Field(i).Set(reflect.ValueOf(ast))
		case "Block":
			var value uint64
			fmt.Sscanln(text, &value)
			v.Field(i).SetUint(value * 1024 * 1024 / config.Global_Shard_Size)
		default:
			fmt.Println("未定义类型", t.Field(i).Type.Name())
		}
	}

	actionData := eos.NewActionData(ad)
	return &actionData, nil
}

func GetSignedTransAction(action *eos.Action, opt *eos.TxOptions) (*eos.PackedTransaction, error) {
	tx := eos.NewTransaction([]*eos.Action{
		action,
	}, opt)

	if len(action.Authorization) == 2 {
		if action.Authorization[0].Actor == action.Authorization[1].Actor {
			action.Authorization = action.Authorization[0:1]
		}
	}

	keysnum := len(action.Authorization)
	fmt.Printf("需要%d个私钥签名\n", keysnum)
	kb := eos.NewKeyBag()
	Api.SetSigner(kb)
	Api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) (keys []ecc.PublicKey, err error) {
		return getPubkey(kb), nil
	})

	for i := 0; i < keysnum; i++ {
	inputKey:
		fmt.Println("请输入私钥:", action.Authorization[i].Actor, action.Authorization[i].Permission)
		var keyValue string
		_, err := fmt.Scanln(&keyValue)
		// 去掉空格
		keyValue = strings.ReplaceAll(keyValue, " ", "")
		if err != nil {
			fmt.Println("输入错误:", err.Error())
			goto inputKey
		}
		// 时间加密
		if keyValue[:2] == "TD" {
			key, err := Decode(keyValue)
			if err != nil {
				return nil, err
			}
			keyValue = key
		}

		kb.ImportPrivateKey(keyValue)
	}

	tx.SetExpiration(time.Minute * 5)
	fmt.Println("action data", action.ActionData.Data)
	sigedTx, packedTx, err := Api.SignTransaction(tx, opt.ChainID, eos.CompressionZlib)
	if err != nil {
		return nil, err
	}

	fmt.Println(sigedTx)
	return packedTx, nil
}

type TransactionRequest struct {
	*eos.PackedTransaction
}

func (tr *TransactionRequest) Send(url string) ([]byte, error) {

	buf, err := json.Marshal(tr.PackedTransaction)
	fmt.Println("packedTx", string(buf))
	if err != nil {
		return nil, err
	}
	fmt.Println("sn url", url)
	res, err := http.Post(url, "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resBuf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return resBuf, fmt.Errorf("status code %d", res.StatusCode)
	}
	return resBuf, nil
}

func NewSignedTransactionRequest(ad interface{}, an string, actn string, auth []eos.PermissionLevel, opt *eos.TxOptions) (*TransactionRequest, error) {
	// 从控制台获取actionData
	actionData, err := GetActionData(ad)
	if err != nil {
		return nil, err
	}
	action := &eos.Action{
		eos.AN(an),
		eos.ActN(actn),
		auth,
		*actionData,
	}

	sigedTx, err := GetSignedTransAction(action, opt)
	if err != nil {
		return nil, err
	}
	return &TransactionRequest{sigedTx}, nil
}
