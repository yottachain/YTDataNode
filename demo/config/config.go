package main

import (
	"github.com/yottachain/YTDataNode/logger"

	"github.com/yottachain/YTDataNode/config"
)

func main() {
	// cfg := config.NewConfig()
	// err := cfg.Save()
	// if err != nil {
	// 	log.Println(err)
	// }
	cfg, err := config.ReadConfig()
	if err != nil {
	}
	log.Println(cfg, err)
}
