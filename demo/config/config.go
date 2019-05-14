package main

import (
	"fmt"

	"github.com/yottachain/YTDataNode/config"
)

func main() {
	// cfg := config.NewConfig()
	// err := cfg.Save()
	// if err != nil {
	// 	fmt.Println(err)
	// }
	cfg, err := config.ReadConfig()
	if err != nil {
	}
	fmt.Println(cfg, err)
}
