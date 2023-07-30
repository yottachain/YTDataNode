package main

import (
	"encoding/json"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"os"
)

func main() {
	fd, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("open bp.json error %s\n", err.Error())
		return
	}

	defer fd.Close()

	decoder := json.NewDecoder(fd)

	cfg, err := config.ReadConfig()
	if err != nil {
		fmt.Printf("open config file fail, error %s\n", err.Error())
		return
	}

	var bpList = config.BPList{}
	err = decoder.Decode(&bpList)
	if err != nil {
		fmt.Printf("decode bp list fail, error %s\n", err.Error())
		return
	}

	cfg.BPList = bpList.BPList

	err = cfg.Save()
	if err != nil {
		fmt.Printf("config file save fail, error %s\n", err.Error())
		return
	}

	return
}
