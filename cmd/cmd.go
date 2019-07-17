package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/cmd/register"
	"github.com/yottachain/YTDataNode/cmd/repo"
	"github.com/yottachain/YTDataNode/cmd/update"
	"github.com/yottachain/YTDataNode/commander"
	"github.com/yottachain/YTDataNode/config"
	"log"
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

func main() {

	initCmd.Flags().Uint64VarP(&size, "size", "s", 4398046511104, "存储空间大小")
	initCmd.Flags().Uint32VarP(&mc, "m", "m", 14, "m的次方（8-20）的偶数")
	daemonCmd.Flags().BoolVarP(&isDaemon, "d", "d", false, "是否在后台运行")

	RootCommand := &cobra.Command{
		Version: fmt.Sprintf("%d", config.Version()),
		Short:   "ytfs storage node",
	}
	RootCommand.AddCommand(initCmd)
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(registerCmd.RegisterCmd)
	RootCommand.AddCommand(repoCmd.RepoCmd)
	RootCommand.AddCommand(update.UpdateCMD)
	RootCommand.Execute()

	err := recover()
	if err != nil {
		log.Println("Error:", err)
	}
}
