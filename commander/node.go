package commander

import (
	"context"
	"fmt"
	"github.com/yottachain/YTDataNode/cmd/update"
	ytfs "github.com/yottachain/YTFS"
	"io"
	"net/http"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"

	"github.com/yottachain/YTDataNode/logger"
	"os"
	"os/exec"
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
	log.Println("YTFS daemon success:", sn.Config().Version())
	for k, v := range sn.Addrs() {
		log.Printf("node addr [%d]:%s/p2p/%s\n", k, v, sn.Host().Config().ID.Pretty())
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

	go func() {

		defer func() {
			err := recover()
			if err != nil {
				log.Println("[install cron]", err)
			}
		}()
		var cronPath = path.Join(util.GetYTFSPath(), "cron-node")
		if exists, err := util.PathExists(cronPath); err != nil || !exists {
			var filename = path.Join(util.GetYTFSPath(), "install_cron.sh")
			os.Remove(filename)
			exec.Command("crontab", "-r").Start()
			log.Println("[cron-node]下载 cron-node")
			resp, err := http.Get("https://yottachain.oss-cn-beijing.aliyuncs.com/yottachain/cron-node-linux-amd64")
			if err != nil {
				log.Println("[cron-node]下载失败")
				return
			}
			defer resp.Body.Close()

			fl, err := os.OpenFile(cronPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_EXCL, 0777)
			if err != nil {
				log.Println("[cron-node]下载失败")
				return
			}
			defer fl.Close()

			io.Copy(fl, resp.Body)
			log.Println("[cron-node]下载 cron-node 完成")
		}

		cfg, err := config.ReadConfig()
		if err != nil {
			log.Println("[cron-node]添加任务失败")
			return
		}
		cmd := exec.Command(cronPath, "daemon")
		err = cmd.Start()
		if err != nil {
			log.Println(err)
		}
		<-time.After(time.Second)
		err = exec.Command(cronPath, "clear").Run()
		if err != nil {
			log.Println(err)
		}
		err = exec.Command(cronPath, "add", fmt.Sprintf("0 * * * * * get report.yottachain.net/%d", cfg.IndexID)).Run()
		if err != nil {
			log.Println(err)
		}
		log.Println("[cron-node]", "添加定时任务完成")
	}()
	<-ctx.Done()
}

func DaemonWithBackground() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	log.SetFileLog()
	var daemonC *exec.Cmd
	go updateService(&daemonC)
	go func() {
		var yOrN byte
		<-sigs
		fmt.Println("Are you sure you want to quit ？（y/n）")
		fmt.Scanf("%c\n", &yOrN)
		if yOrN == 'y' {
			daemonC.Process.Signal(syscall.SIGQUIT)
			os.Exit(0)
		}
	}()
	for {
		daemonC = getDaemonCmd()
		log.Println("启动进程daemon")
		err := daemonC.Run()
		log.Println("重启完成")
		if err != nil {
			log.Println(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func getDaemonCmd() *exec.Cmd {
	file := log.FileLogger
	var daemonC *exec.Cmd
	daemonC = exec.Command(os.Args[0], "daemon")
	daemonC.Env = os.Environ()
	daemonC.Stdout = file
	daemonC.Stderr = file
	return daemonC
}

func updateService(c **exec.Cmd) {
	log.Println("自动更新服务启动")
	for {
		dcmd := *c

		time.Sleep(time.Minute * 10)
		log.Println("尝试更新")
		if err := update.Update(); err == nil {
			log.Println("更新完成尝试重启")
			reboot(dcmd.Process.Pid)
		} else {
			log.Println(err)
		}
	}
}

func reboot(pid int) {
	rebootShell := fmt.Sprintf("kill -9 %d;kill -9 %d;%s daemon -d &", os.Getpid(), pid, os.Args[0])
	execPath, err := GetCurrentPath()
	if err != nil {
		log.Println("[auto update]重启失败", err)
	}
	rebootShellPath := path.Join(execPath, "reboot.sh")

	file, err := os.OpenFile(rebootShellPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777)
	if err == nil {
		_, err := io.WriteString(file, rebootShell)
		if err == nil {
			rebootCMD := exec.Command("bash", rebootShellPath)
			rebootCMD.Stdout = log.FileLogger
			rebootCMD.Stderr = log.FileLogger
			if err := rebootCMD.Start(); err != nil {
				log.Println("[auto update]重启失败", err)
			}
		} else {
			log.Println("[auto update]重启失败", err)
		}
	} else {
		log.Println("[auto update]重启失败", err)
	}
}

func GetCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	return filepath.Dir(path), nil
}
