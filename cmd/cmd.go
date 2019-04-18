package main

import (
	"context"
	"fmt"
	"yottachain/ytfs-storage-node"

	"github.com/spf13/cobra"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "YTFS storage node running daemon",
	Run: func(cmd *cobra.Command, args []string) {
		sn, err := node.NewStorageNode()
		if err != nil {
			panic("YTFS init fail")
		}
		fmt.Println("YTFS init success")
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
