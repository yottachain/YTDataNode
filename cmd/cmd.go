package main

import (
	"github.com/yottachain/YTDataNode/cmd/register"
	"log"

	"github.com/yottachain/YTDataNode/commander"

	"github.com/spf13/cobra"
)

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
		commander.Init()
		log.Println("YTFS init success")
	},
}

var idCmd = &cobra.Command{
	Use:   "id",
	Short: "ID api",
}

var newIDCmd = &cobra.Command{
	Use:   "new",
	Short: "create new id",
	Run: func(cmd *cobra.Command, args []string) {
		id, index := commander.NewID()
		log.Println("create new id success")
		log.Printf("id:%s sn:%d\n", id, index)
	},
}

func main() {
	RootCommand := &cobra.Command{
		Short: "ytfs storage node",
	}
	idCmd.AddCommand(newIDCmd)
	RootCommand.AddCommand(initCmd)
	RootCommand.AddCommand(daemonCmd)
	RootCommand.AddCommand(idCmd)
	RootCommand.AddCommand(registerCmd.RegisterCmd)
	RootCommand.Execute()

	err := recover()
	if err != nil {
		log.Println("Error:", err)
	}
}
