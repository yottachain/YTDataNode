package Transaction

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"net/http"
	"os"
	"reflect"
)

// 新增的通用api

func getPubkey(kb *eos.KeyBag) []ecc.PublicKey {
	var pkeys = make([]ecc.PublicKey, len(kb.Keys))
	for k, v := range kb.Keys {
		pkeys[k] = v.PublicKey()
	}
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
		}
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		text := scanner.Text()
		if required == "true" && text == "" {
			fmt.Println("该字段不能为空")
			goto input
		}
		fmt.Println(text, required)
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
	fmt.Println("需要%d个私钥签名", keysnum)
	kb := eos.NewKeyBag()
	for i := 0; i < keysnum; i++ {
	inputKey:
		fmt.Println("请输入私钥:", action.Authorization[i].Actor)
		var keyValue string
		_, err := fmt.Scanln(&keyValue)
		if err != nil {
			fmt.Println("输入错误:", err.Error())
			goto inputKey
		}
		kb.ImportPrivateKey(keyValue)
	}

	sigedTx, err := kb.Sign(sigedTx, opt.ChainID, getPubkey(kb)...)
	if err != nil {
		return nil, err
	}

	return sigedTx, nil
}

type TransactionRequest struct {
	*eos.SignedTransaction
}

func (tr *TransactionRequest) Send(url string) (*http.Response, error) {
	packedTx, err := tr.SignedTransaction.Pack(eos.CompressionZlib)
	if err != nil {
		return nil, err
	}
	buf, err := json.Marshal(packedTx)
	fmt.Println(buf)
	if err != nil {
		return nil, err
	}
	res, err := http.Post(url, "applaction/json", bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	return res, nil
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
