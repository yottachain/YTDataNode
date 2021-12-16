package main

import (
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
)

func main() {
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
	}

	log.Println("change success")
}
