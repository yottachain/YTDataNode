package main

import (
	"fmt"
	"github.com/yottachain/YTDataNode/cmd/register"
	"github.com/yottachain/YTDataNode/cmd/repo"
	"github.com/yottachain/YTDataNode/config"
	"log"

	"github.com/yottachain/YTDataNode/commander"

	"github.com/spf13/cobra"
)

var size uint64
var mc uint32
var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "YTFS storage node running daemon",
	Run: func(cmd *cobra.Command, args []string) {
		// sn, err := node.NewStorageNode(args[0])
		// if err != nil {
		// 	log.Println("ytfs-disk init fail", err)
		// }
		// sn.Host().Daemon(context.Background(), "/ip4/0.0.0.0/tcp/9001")
		// err = sn.Host().ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{
		// 	"/ip4/172.21.0.13/tcp/9999",
		// 	"/ip4/152.136.11.202/tcp/9999",
		// })
		// if err != nil {
		// 	log.Println("Add addr fail", err)
		// }
		// log.Println("pk:", args[0])
		// log.Println("YTFS init success")
		// for k, v := range sn.Host().Addrs() {
		// 	log.Printf("node addr [%d]:%s/p2p/%s\n", k, v, sn.Host().ID().Pretty())
		// }
		// srv := api.NewHTTPServer(sn)
		// log.Println("Wait request")
		// sn.Service()
		// go func() {
		// 	if err := srv.Daemon(":9002"); err != nil {
		// 		panic(fmt.Sprintf("Api server fail:%s\n", err))
		// 	} else {
		// 		log.Printf("API serve at:%s\n", srv.Addr)
		// 	}
		// }()
		// ctx := context.Background()
		// <-ctx.Done()
		commander.Daemon()
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

	RootCommand := &cobra.Command{
		Version: fmt.Sprintf("%d", config.Version()),
		Short:   "ytfs storage node",
	}
	RootCommand.AddCommand(initCmd)
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(registerCmd.RegisterCmd)
	RootCommand.AddCommand(repoCmd.RepoCmd)
	RootCommand.Execute()

	err := recover()
	if err != nil {
		log.Println("Error:", err)
	}
}
