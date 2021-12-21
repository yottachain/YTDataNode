package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
)

//主网bp
var bplist = `
    [
		{
		  "ID": "16Uiu2HAm7o24DSgWTrcu5sLCgSkf3D3DQqzpMz9W1Bi7F2Cc4SF6",
		  "Addrs": ["/dns4/sn.yottachain.net/tcp/9999"]
		}
	]`

var formPath = ""
//主网改成一个sn后 修改矿机的snlist和清空矿机的数据
func main() {
	flag.StringVar(&formPath, "f", "", "sn列表")

	cfg, err := config.ReadConfig()
	if err != nil {
		log.Println("change read config fail")
		return
	}
	fs, err := ytfs.OpenGet(util.GetYTFSPath(), cfg.Options)
	if err != nil {
		log.Printf("change open ytfs err:%s\n", err.Error())
	}
	if fs != nil {
		err = fs.ModifyPos(5)
		if err != nil {
			log.Printf("change ytfs pos err:%s\n", err.Error())
		}
		fs.Close()
	}else {
		log.Printf("change open ytfs nil\n")
		return
	}

	log.Println("change success")

	buf := bytes.NewBufferString(bplist)
	err = json.Unmarshal(buf.Bytes(), &cfg.BPList)
	if err != nil {
		log.Printf("cfg update err: %s\n", err.Error())
		return
	}
	_ = cfg.Save()
	log.Println("cfg update success")
}
