package main

import (
	"bufio"
	"fmt"
	"github.com/yottachain/YTDataNode/diskHash"

	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/cmd/account"
	registerCmd "github.com/yottachain/YTDataNode/cmd/register"
	repoCmd "github.com/yottachain/YTDataNode/cmd/repo"
	"github.com/yottachain/YTDataNode/cmd/update"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	//comm "github.com/yottachain/YTFS/common"
)

var size uint64
var mc uint32
var db string
var stortype uint32
var devname string
var isDaemon bool = false

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "YTFS storage node running daemon",
	Run: func(cmd *cobra.Command, args []string) {

		if isDaemon {
			commander.DaemonWithBackground()
		} else {
			commander.Daemon()
		}
	},
}
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "以守护进程启动并且自动调起掉线程序",
	Run: func(cmd *cobra.Command, args []string) {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
		c := exec.Command(os.Args[0], "daemon", "-d")
		c.Env = os.Environ()
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		err := c.Start()
		if err != nil {
			log.Println("进程启动失败:", err)
		} else {
			log.Println("守护进程已启动")
			log.Println("日志输出在output.log")
		}
	},
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Init YTFS storage node",
	Run: func(cmd *cobra.Command, args []string) {
		cfg, err := config.ReadConfig()
		if err != nil {
			log.Println("YTFS init failed err:", err)
			return
		}
		yt, err := ytfs.OpenInit(util.GetYTFSPath(), cfg.Options)
		if err != nil {
			log.Println("YTFS init failed")
			return
		}
		defer yt.Close()

		l := yt.PosIdx()
		if l < 5 {
			log.Println("[diskHash] ytfs_len:", l)
			err := diskHash.RandWrite(yt, uint(l))
			if err != nil {
				log.Println("[diskHash] randWrite to ytfs error:", err)
				return
			}
		}
		fmt.Println("YTFS init success")
	},
}

//var version = &cobra.Command{
//	Use:   "version",
//	Short: "ytfs-node version",
//	Run: func(cmd *cobra.Command, args []string) {
//		log.Printf("ytfs-node version:%d\n", config.Version())
//	},
//}

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "log print",
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := net.Dial("tcp", "127.0.0.1:9003")
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	},
}

var regTemplateCmd = &cobra.Command{
	Use:   "register-form",
	Short: "生成注册表单",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(registerCmd.GetFormTemplate())
	},
}

func main() {
	//initCmd.Flags().Uint64VarP(&size, "size", "s", 4398046511104, "存储空间大小")
	//initCmd.Flags().Uint32VarP(&mc, "order", "k", 14, "N = (1<<k), 其中k的值（8-20）")
	//initCmd.Flags().StringVar(&db, "db", "indexdb", "数据库选择, indexdb or rocksdb")
	//initCmd.Flags().Uint32VarP(&stortype,"type","t",0,"选择存储类型,0-文件(默认),1-块设备")
	//initCmd.Flags().StringVarP(&devname,"name","n","storage","存储设备的名称:storage(默认)")

	daemonCmd.Flags().BoolVarP(&isDaemon, "d", "d", false, "是否在后台运行")

	RootCommand := &cobra.Command{
		Version: fmt.Sprintf("%s", "1.0.15v"),
		Short:   "ytfs storage node",
	}
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(registerCmd.RegisterCmd)
	RootCommand.AddCommand(repoCmd.RepoCmd)
	RootCommand.AddCommand(update.UpdateCMD)
	RootCommand.AddCommand(logCmd)
	RootCommand.AddCommand(account.AccountCmd)
	RootCommand.AddCommand(regTemplateCmd)
	RootCommand.AddCommand(initCmd)
	//RootCommand.AddCommand(startCmd)
	RootCommand.Execute()
}
