package main

import (
	"context"
	"fmt"

	"github.com/yottachain/YTDataNode"

	"github.com/spf13/cobra"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "YTFS storage node running daemon",
	Run: func(cmd *cobra.Command, args []string) {
		sn, err := node.NewStorageNode(args[0])
		if err != nil {
			fmt.Println("ytfs-disk init fail", err)
		}
		fmt.Println("pk:", args[0])
		fmt.Println("YTFS init success")
		fmt.Println("node ID", sn.Host().ID().Pretty())
		fmt.Println("Wait request")
		sn.Service()
		ctx := context.Background()
		<-ctx.Done()
	},
}

func main() {
	RootCommand := &cobra.Command{
		Short: "ytfs storage node",
	}
	RootCommand.AddCommand(daemonCmd)
	RootCommand.Execute()
}
