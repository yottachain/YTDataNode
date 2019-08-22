package main

import (
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logger"
)

func main() {
	cfg := config.NewConfig()
	log.Println(cfg.BPList)
}
