package commander

import (
	"context"
	"fmt"

	"github.com/yottachain/YTDataNode"
	"github.com/yottachain/YTDataNode/api"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
)

// Init 初始化
func Init() error {
	cfg := config.NewConfig()
	cfg.Save()
	yt, err := ytfs.Open(util.GetYTFSPath(), cfg.Options)

	if err != nil {
		return err
	}
	defer yt.Close()
	return nil
}

// Daemon 启动守护进程
func Daemon() {
	ctx := context.Background()
	cfg, err := config.ReadConfig()
	if err != nil {
		panic(err)
	}
	sn, err := node.NewStorageNode(cfg)
	if err != nil {
		panic(err)
	}
	err = sn.Host().Daemon(ctx, cfg.ListenAddr)
	if err != nil {
		fmt.Println("node daemon fail", err)
	}
	err = sn.Host().ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{
		"/ip4/172.21.0.13/tcp/9999",
		"/ip4/152.136.11.202/tcp/9999",
	})
	if err != nil {
		fmt.Println("Add addr fail", err)
	}
	fmt.Println("YTFS daemon success")
	for k, v := range sn.Host().Addrs() {
		fmt.Printf("node addr [%d]:%s/p2p/%s\n", k, v, sn.Host().ID().Pretty())
	}
	srv := api.NewHTTPServer(sn)
	fmt.Println("Wait request")
	sn.Service()
	go func() {
		if err := srv.Daemon(cfg.APIListen); err != nil {
			panic(fmt.Sprintf("Api server fail:%s\n", err))
		} else {
			fmt.Printf("API serve at:%s\n", srv.Addr)
		}
	}()
	defer sn.YTFS().Close()
	<-ctx.Done()
}
