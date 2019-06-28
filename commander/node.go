package commander

import (
	"context"
	"fmt"
	"log"

	ytfs "github.com/dp1993132/YTFS"
	// node "github.com/yottachain/YTDataNode"
	"github.com/yottachain/YTDataNode/api"
	"github.com/yottachain/YTDataNode/config"
	instance "github.com/yottachain/YTDataNode/instance"
	"github.com/yottachain/YTDataNode/util"
)

// Init 初始化
func Init() error {
	cfg := config.NewConfig()
	cfg.Save()
	yt, err := ytfs.NewYTFS(util.GetYTFSPath(), cfg.Options)

	if err != nil {
		return err
	}
	defer yt.Close()
	return nil
}

// NewID 创建新的id
func NewID() (string, int) {
	cfg, err := config.ReadConfig()
	if err != nil {
		log.Println("read config fail:", err)
	}
	cfg.NewKey()
	cfg.Save()
	return cfg.ID, cfg.GetBPIndex()
}

// Daemon 启动守护进程
func Daemon() {
	ctx := context.Background()
	sn := instance.GetStorageNode()
	err := sn.Host().Daemon(ctx, sn.Config().ListenAddr)
	if err != nil {
		log.Println("node daemon fail", err)
	}
	log.Println("YTFS daemon success")
	for k, v := range sn.Addrs() {
		log.Printf("node addr [%d]:%s/p2p/%s\n", k, v, sn.Host().ID().Pretty())
	}
	srv := api.NewHTTPServer()
	log.Println("Wait request")
	sn.Service()
	go func() {
		if err := srv.Daemon(); err != nil {
			panic(fmt.Sprintf("Api server fail:%s\n", err))
		} else {
			log.Printf("API serve at:%s\n", srv.Addr)
		}
	}()
	defer sn.YTFS().Close()
	<-ctx.Done()
}
