package main

import (
	"bufio"
	"fmt"

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
	log "github.com/yottachain/YTDataNode/logger"
)

var size uint64
var mc uint32
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
		//defer func() {
		//	if err := recover(); err != nil {
		//		log.Println(err)
		//	}
		//}()
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
		commander.InitBySignleStorage(size, 1<<mc)
		log.Println("YTFS init success")
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

func main() {
	//defer func() {
	//	err := recover()
	//	if err != nil {
	//		log.Println("Error:", err)
	//	}
	//}()
	initCmd.Flags().Uint64VarP(&size, "size", "s", 4398046511104, "存储空间大小")
	initCmd.Flags().Uint32VarP(&mc, "m", "m", 14, "m的次方（8-20）的数")
	daemonCmd.Flags().BoolVarP(&isDaemon, "d", "d", false, "是否在后台运行")

	RootCommand := &cobra.Command{
		Version: fmt.Sprintf("%s", "1.0.6w"),
		Short:   "ytfs storage node",
	}
	RootCommand.AddCommand(initCmd)
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(registerCmd.RegisterCmd)
	RootCommand.AddCommand(repoCmd.RepoCmd)
	RootCommand.AddCommand(update.UpdateCMD)
	RootCommand.AddCommand(logCmd)
	RootCommand.AddCommand(account.AccountCmd)
	//RootCommand.AddCommand(startCmd)
	RootCommand.Execute()
}
