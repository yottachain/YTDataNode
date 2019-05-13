package main

import (
	"context"
	"fmt"

	"github.com/yottachain/YTDataNode/api"

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
		sn.Host().Daemon(context.Background(), "/ip4/0.0.0.0/tcp/9001")
		err = sn.Host().ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{
			"/ip4/172.21.0.13/tcp/9999",
			"/ip4/152.136.11.202/tcp/9999",
		})
		if err != nil {
			fmt.Println("Add addr fail", err)
		}
		fmt.Println("pk:", args[0])
		fmt.Println("YTFS init success")
		for k, v := range sn.Host().Addrs() {
			fmt.Printf("node addr [%d]:%s/p2p/%s\n", k, v, sn.Host().ID().Pretty())
		}
		srv := api.NewHTTPServer(sn)
		fmt.Println("Wait request")
		sn.Service()
		go func() {
			if err := srv.Daemon(":9002"); err != nil {
				panic(fmt.Sprintf("Api server fail:%s\n", err))
			} else {
				fmt.Printf("API serve at:%s\n", srv.Addr)
			}
		}()
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
