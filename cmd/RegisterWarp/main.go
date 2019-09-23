package main

import (
	"bufio"
	"fmt"
	"github.com/spf13/viper"
	log "github.com/yottachain/YTDataNode/logger"
	"os"
	"os/exec"
)

var maxSpace int64 = 268435456
var key1 string // 抵押私钥
var key2 string // 矿机管理员私钥
var key3 string // 矿池私钥

var depAN string        //抵押账号名
var adminAN string      //管理员账号名
var poolAdminAN string  //矿池名
var beneficialAN string //收益账号
var envs []string

var configPath string
var ytfsNodePath string
var ytfsSignerPath string

func main() {
	log.Println("最大空间:", maxSpace)
	log.Println("抵押账号:", depAN, "-", key1)
	log.Println("矿机管理员:", adminAN, "-", key2)
	log.Println("矿池管理员:", poolAdminAN, "-", key3)
	log.Println("收益账号:", beneficialAN)
	log.Println("ytfs-node 路径:", ytfsNodePath)
	log.Println("ytfs-signer 路径:", ytfsSignerPath)
	log.Println("开始注册...")
	register()
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
	beneficialAN = viper.GetString("beneficial.name")

	ytfsNodePath = viper.GetString("ytfsNodePath")
	ytfsSignerPath = viper.GetString("signerPath")

	envs = viper.GetStringSlice("env")
}

func register() {
	//cmdin, _ := os.OpenFile(path.Join(path.Dir(configPath), "in"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	//mcin := io.MultiWriter(os.Stdout, cmdin)
	var next bool = false
	var actionStr chan string = make(chan string)

	cmd := exec.Command(ytfsNodePath, "register")
	cmd.Env = envs
	//cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	out, _ := cmd.StdoutPipe()
	scaner := bufio.NewScanner(out)
	go func() {
		for scaner.Scan() {
			if next {
				actionStr <- scaner.Text()
			}
			if scaner.Text() == "请对如下交易进行签名并粘贴:" {
				next = true
			}
			fmt.Println("-", scaner.Text())
		}
	}()

	w, _ := cmd.StdinPipe()
	cmd.Start()
	fmt.Fprintf(w, "%c\n", 'y')
	fmt.Fprintf(w, "%d\n", maxSpace*16/1024/1024)
	fmt.Fprintf(w, "%d\n", 14)
	fmt.Fprintln(w, depAN)
	fmt.Fprintln(w, maxSpace*16*1024)
	fmt.Fprintln(w, adminAN)

	select {
	case act := <-actionStr:
		println(act)
		res := signer(act, key1)
		fmt.Fprintf(w, "%s\n", res)
		fmt.Fprintf(w, "%c\n", 'y')
	}
	cmd.Wait()
}

func signer(tx string, keys ...string) string {
	var signedTx chan string = make(chan string)
	var next bool = false
	cmd := exec.Command(ytfsSignerPath, keys...)
	out, _ := cmd.StdoutPipe()
	w, _ := cmd.StdinPipe()
	scaner := bufio.NewScanner(out)
	go func() {
		for scaner.Scan() {
			if next {
				fmt.Println("-----------签名结果-----------:")
				fmt.Println(scaner.Text())
				signedTx <- scaner.Text()
			}
			if scaner.Text() == "-----------签名结果-----------" {
				next = true
			}
		}
	}()
	cmd.Start()
	fmt.Fprintln(w, tx)
	return <-signedTx
}
