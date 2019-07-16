package main

import (
	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/cmd/daemon"
)

var RootCMD = &cobra.Command{
	Short: "YOTTACHAIN 矿机管理程序",
}

func main() {
	daemon.InitConfig()
	RootCMD.AddCommand(daemon.DaemonCMD)
	RootCMD.AddCommand(daemon.UpdateCMD)
	RootCMD.Execute()
}
