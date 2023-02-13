package main

import (
	"bufio"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/diskHash"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	ytfsutil "github.com/yottachain/YTFS/util"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/cmd/account"
	registerCmd "github.com/yottachain/YTDataNode/cmd/register"
	"github.com/yottachain/YTDataNode/cmd/update"
	"github.com/yottachain/YTDataNode/commander"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/slicecompare"
)

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
	Short: "Init or reInit YTFS storage node",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("use --yes-init to init")
	},
}

var confirmInit = &cobra.Command{
	Use:   "--yes-init",
	Short: "confirm init YTFS storage node",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("delete compare db...")
		//first del slicecompare directory
		ytfsutil.DelPath(util.GetYTFSPath() + slicecompare.Comparedb)
		fmt.Printf("use --yes-init--yes to final init")
	},
}

var confirmYesInit = &cobra.Command{
	Use:   "--yes-init--yes",
	Short: "again confirm init YTFS storage node",
	Run: func(cmd *cobra.Command, args []string) {
		//first del slicecompare directory
		ytfsutil.DelPath(util.GetYTFSPath() + slicecompare.Comparedb)

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
	daemonCmd.Flags().BoolVarP(&isDaemon, "d", "d", false, "是否在后台运行")

	RootCommand := &cobra.Command{
		Version: fmt.Sprintf("%s", "1.0.16b"),
		Short:   "ytfs storage node",
	}
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(registerCmd.RegisterCmd)
	//RootCommand.AddCommand(repoCmd.RepoCmd)
	RootCommand.AddCommand(update.UpdateCMD)
	RootCommand.AddCommand(logCmd)
	RootCommand.AddCommand(account.AccountCmd)
	RootCommand.AddCommand(regTemplateCmd)
	RootCommand.AddCommand(initCmd)
	initCmd.AddCommand(confirmInit)
	confirmInit.AddCommand(confirmYesInit)

	RootCommand.Execute()
}
