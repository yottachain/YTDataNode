package main

import (
	"github.com/spf13/viper"
	log "github.com/yottachain/YTDataNode/logger"
	"os"
)

var maxSpace int64 = 268435456
var key1 string // 抵押私钥
var key2 string // 矿机管理员私钥
var key3 string // 矿池私钥

var depAN string       //抵押账号名
var adminAN string     //管理员账号名
var poolAdminAN string //矿池名

var configPath string

func main() {
	log.Println(maxSpace, depAN, adminAN, poolAdminAN, key1, key2, key3)
}

func init() {
	configPath = os.Args[1]
	file, err := os.OpenFile(configPath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	viper.SetConfigType("yaml")
	err = viper.ReadConfig(file)
	if err != nil {
		log.Fatalln(err)
	}
	maxSpace = viper.GetInt64("maxSpace")
	if maxSpace <= 0 {
		maxSpace = 268435456
	}
	key1 = viper.GetString("dep.key")
	key2 = viper.GetString("miner.key")
	key3 = viper.GetString("pool.key")

	depAN = viper.GetString("dep.name")
	adminAN = viper.GetString("miner.name")
	poolAdminAN = viper.GetString("pool.name")
}
