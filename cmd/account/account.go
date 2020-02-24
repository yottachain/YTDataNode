package account

import (
	"github.com/eoscanada/eos-go"
)

var baseNodeUrl = "http://dnapi1.yottachain.net:8888" //正式
//var baseNodeUrl = "http://124.156.54.96:8888" //测试

var api = eos.New(baseNodeUrl)
