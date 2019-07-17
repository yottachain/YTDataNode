package commander

import (
	"context"
	ytfs "github.com/yottachain/YTFS"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"syscall"
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
	var file *os.File
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)

	file, err := os.OpenFile(path.Join(util.GetYTFSPath(), "output.log"), os.O_WRONLY, 0666)
	if err != nil {
		fi, err := os.Create(path.Join(util.GetYTFSPath(), "output.log"))
		if err != nil {
			log.Fatalln("打开日志文件失败", err)
		}
		file = fi
	}

	defer file.Close()
	c := exec.Command(os.Args[0], "daemon")
	c.Env = os.Environ()
	c.Stdout = file
	c.Stderr = file
	err = c.Start()
	if err != nil {
		log.Fatalln("启动失败：", err)
	} else {
		log.Println("守护进程已启动，进程ID：", c.Process.Pid)
	}
	log.Println(c.ProcessState.Exited())
	os.Exit(0)
}
