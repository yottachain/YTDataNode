package main

import (
	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/daemon"
)

var RootCMD = &cobra.Command{
	Short: "YOTTACHAIN 矿机管理程序",
}

func main() {
	RootCMD.AddCommand(daemon.DaemonCMD)
	RootCMD.Execute()
}
