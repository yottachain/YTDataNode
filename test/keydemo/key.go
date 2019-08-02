package main

import (
	"log"
	"github.com/yottachain/YTDataNode/config"
)

func main() {
	cfg := config.NewConfig()
	log.Println(cfg.BPList)
}
