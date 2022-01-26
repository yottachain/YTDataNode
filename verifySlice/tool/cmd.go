package main

import (
	"github.com/spf13/cobra"
	log "github.com/yottachain/YTDataNode/logger"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

var daemonCmd = &cobra.Command{
	Use:   "daemom",
	Short: "以守护进程启动程序",
	Run: func(cmd *cobra.Command, args []string) {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
		c := exec.Command(os.Args[0], "start")
		c.Env = os.Environ()
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		err := c.Start()
		if err != nil {
			log.Println("进程启动失败:", err)
		} else {
			log.Println("守护进程已启动")
		}
	},
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "前台运行程序",
	Run: func(cmd *cobra.Command, args []string) {
		start()
	},
}

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "查询校验的状态",
	Run: func(cmd *cobra.Command, args []string) {
		verifyStatus()
	},
}

func main () {
	Loop = startCmd.Flags().Bool("l",true,"verify mode :loop or not")
	startCmd.Flags().StringVar(&StartItem,"s","","start items to verify")
	startCmd.Flags().StringVar(&CntPerBatch,"c","1000","verify items for one batch")
	startCmd.Flags().StringVar(&BatchCnt,"b","1000","batch count for verify")

	checkCmd.Flags().StringVar(&VerifyErrKey,"key","","Get verify status for verified-error key")

	log.SetFileLog()

	RootCommand := &cobra.Command{
		Short:   "ytfs verify",
	}
	RootCommand.AddCommand(startCmd)
	RootCommand.AddCommand(checkCmd)
	RootCommand.AddCommand(daemonCmd)

	RootCommand.Execute()
}
