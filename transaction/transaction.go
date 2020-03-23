package transaction

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"
)

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
	fmt.Println("pk", pkeys)
	fmt.Println("keys", kb.Keys)
	return pkeys
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
		switch t.Field(i).Type.Name() {
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
			case "yes":
				v.Field(i).SetBool(true)
			case "no":
				v.Field(i).SetBool(true)
			default:
				fmt.Println("输入错误")
				goto input
			}
		case "Asset":
			var value int64
			fmt.Sscanln(text, &value)
			ast := newYTAAssect(value)
			v.Field(i).Set(reflect.ValueOf(ast))
		default:
			fmt.Println("未定义类型", t.Field(i).Type.Name())
		}
	}

	actionData := eos.NewActionData(ad)
	return &actionData, nil
}

func GetSignedTransAction(action *eos.Action, opt *eos.TxOptions) (*eos.SignedTransaction, error) {
	tx := eos.NewTransaction([]*eos.Action{
		action,
	}, opt)

	sigedTx := eos.NewSignedTransaction(tx)

	keysnum := len(action.Authorization)
	fmt.Printf("需要%d个私钥签名\n", keysnum)
	kb := eos.NewKeyBag()
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
		//kb.ImportPrivateKey(keyValue)
		kb.ImportPrivateKey("5JkjKo4UGaTQFVuVpDZDV3LNvLrd2DgGRpTNB4E1o9gVuUf7aYZ")
	}

	fmt.Println("kb", kb.Keys)
	sigedTx, err := kb.Sign(sigedTx, opt.ChainID, getPubkey(kb)...)
	if err != nil {
		return nil, err
	}

	return sigedTx, nil
}

type TransactionRequest struct {
	*eos.SignedTransaction
}

func (tr *TransactionRequest) Send(url string) ([]byte, error) {
	tr.SetExpiration(30 * time.Minute)
	fmt.Printf("%v\n", tr)
	packedTx, err := tr.SignedTransaction.Pack(eos.CompressionZlib)
	if err != nil {
		return nil, err
	}
	buf, err := json.Marshal(packedTx)
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
