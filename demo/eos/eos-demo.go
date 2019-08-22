package main

import (
	"github.com/eoscanada/eos-go"
	//"github.com/eoscanada/eos-go/ecc"
)

func main() {
	api := eos.New("http://35.176.59.89:8888")
	kb := eos.NewKeyBag()
	kb.Add("私钥")

	api.SetSigner(kb)
	action := eos.Action{}

	opt := eos.TxOptions{}
	opt.FillFromChain(api)

	tx := eos.NewTransaction([]*eos.Action{&action}, &opt)

	api.SignPushTransaction(tx, opt.ChainID, eos.CompressionNone)
}
