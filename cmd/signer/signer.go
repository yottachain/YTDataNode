package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	"github.com/spf13/viper"
	"path"

	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	"os"
	//"github.com/rocket049/gocui"
)

var key string
var key2 bool
var keys []string
var tx string

//var baseNodeUrl = "http://35.176.59.89:8888"

var baseNodeUrl = "http://dnapi1.yottachain.net:8888" //测试

var api = eos.New(baseNodeUrl)
var kb = eos.NewKeyBag()

func init() {
	filename := path.Join(util.GetYTFSPath(), "debug.yaml")
	ok, err := util.PathExists(filename)
	if err == nil && ok {
		viper.SetConfigType("yaml")
		fl, err := os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer fl.Close()
		viper.ReadConfig(fl)
		fmt.Println("---------DEBUG MODE------")
		baseNodeUrl = viper.GetString("baseNodeUrl")
		fmt.Println("baseNodeUrl:", baseNodeUrl)
		api = eos.New(baseNodeUrl)
	}
}

func main() {
	var signedTx eos.SignedTransaction
	flag.StringVar(&key, "k", "", "签名私钥")
	flag.BoolVar(&key2, "k2", false, "签名私钥")
	flag.StringVar(&tx, "t", "", "签名交易")
	flag.Parse()

	if key == "" && len(os.Args) > 1 {
		keys = os.Args[1:]
		for _, v := range keys {
			kb.ImportPrivateKey(v)
		}
	}
	kb.ImportPrivateKey(key)
	if key2 == true {
		kb.ImportPrivateKey("5JkjKo4UGaTQFVuVpDZDV3LNvLrd2DgGRpTNB4E1o9gVuUf7aYZ")
	}
	if tx == "" {
		log.Println("请输入待签名交易：")
		tx = util.ReadStringLine(os.Stdin, 4096)
	}
	err := json.Unmarshal([]byte(tx), &signedTx)
	if err != nil {
		log.Println("签名失败:", err)
	}
	txopts := &eos.TxOptions{}
	txopts.FillFromChain(api)

	res, err := kb.Sign(&signedTx, txopts.ChainID, getPubkey()...)
	if err != nil {
		log.Println("签名失败:", err)
	}
	fmt.Println("交易签名：")
	buf, _ := json.Marshal(res)
	fmt.Println("-----------签名结果-----------")
	fmt.Println(string(buf))
	fmt.Println("-----------------------------")
}

func getPubkey() []ecc.PublicKey {
	var pkeys = make([]ecc.PublicKey, len(kb.Keys))
	for k, v := range kb.Keys {
		pkeys[k] = v.PublicKey()
	}
	return pkeys
}
