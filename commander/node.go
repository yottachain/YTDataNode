package commander

import (
	"context"
	"github.com/yottachain/YTDataNode/cmd/update"
	ytfs "github.com/yottachain/YTFS"

	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	// node "github.com/yottachain/YTDataNode"
	"github.com/yottachain/YTDataNode/api"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/instance"
	"github.com/yottachain/YTDataNode/util"
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

func InitBySignleStorage(size uint64, m uint32) error {
	cfg := config.NewConfigByYTFSOptions(config.GetYTFSOptionsByParams(size, m))
	cfg.Save()
	yt, err := ytfs.Open(util.GetYTFSPath(), cfg.Options)
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
			log.Fatalf("Api server fail %s\n", err)
		} else {
			log.Printf("API serve at:%s\n", srv.Addr)
		}
	}()
	defer sn.YTFS().Close()
	<-ctx.Done()
}

func DaemonWithBackground() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)

	var daemonC *exec.Cmd

	go updateService(daemonC)

	for {
		daemonC = getDaemonCmd()
		log.Println("启动进程daemon")
		err := daemonC.Run()
		if err != nil {
			log.Println(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func getDaemonCmd() *exec.Cmd {
	file := util.GetLogFile("output.log")
	var daemonC *exec.Cmd
	daemonC = exec.Command(os.Args[0], "daemon")
	daemonC.Env = os.Environ()
	daemonC.Stdout = file
	daemonC.Stderr = file
	return daemonC
}

func updateService(c *exec.Cmd) {
	log.Println("自动更新服务启动")
	for {
		time.Sleep(time.Minute * 10)
		log.Println("尝试更新")
		if err := update.Update(); err == nil {
			log.Println("更新完成尝试重启")
			c.Process.Kill()
		} else {
			log.Println(err)
		}
	}
}
