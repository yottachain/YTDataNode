package commander

import (
	"bytes"
	"context"
	"fmt"
	"github.com/yottachain/YTDataNode/cmd/update"
	"github.com/yottachain/YTDataNode/diskHash"
	ytfs "github.com/yottachain/YTFS"
	"io"
	"net/http"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
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
	fmt.Println("[init] node Init")
	var cfg *config.Config
	if  !CfgFileExist(){
		cfg = config.NewConfig()
		cfg.Save()
	}else {
		var err error
		cfg, err = config.ReadConfig()
		if err != nil {
			fmt.Println("[init] InitBySignleStorage error:", err.Error())
			return err
		}
	}

	yt, err := ytfs.Open(util.GetYTFSPath(), cfg.Options, cfg.IndexID)
	if err != nil {
		return err
	}
	defer yt.Close()
	return nil
}

func CfgFileExist() bool {
	cfgPath := util.GetConfigPath()
	bl, _ := util.PathExists(cfgPath)
	return bl
}

func Check2Orders(num uint32) bool{
	var ret = false
	var order = uint32(1)
	for {
		if num == (1<<order){
			ret = true
			break
		}

		if order > 32 {
			break
		}

		order++
	}

	return ret
}

func InitBySignleStorage(size uint64, n uint32, db string) error {
	fmt.Println("[init]node InitBySignleStorage")
	var cfg *config.Config

	//cfg = config.NewConfigByYTFSOptions(config.GetYTFSOptionsByParams(size, mc))
	if !CfgFileExist(){
		cfg = config.NewConfigByYTFSOptions(config.GetYTFSOptionsByParams(size, n, db))
		if cfg == nil{
			err := fmt.Errorf("cfg is nil")
			fmt.Println("[error] ",err.Error())
			return err
		}
		cfg.Save()
	}else{
		var err error
		cfg, err = config.ReadConfig()
		if err != nil{
			fmt.Println("[init] InitBySignleStorage error:",err.Error())
			return err
		}
	}

	if !cfg.Options.UseKvDb {
		if cfg.Options.IndexTableCols > 2048 || cfg.Options.IndexTableCols < 512{
			err := fmt.Errorf("IndexTableCols(M) not suitable, M=",cfg.Options.IndexTableCols)
			fmt.Println("[error] ",err.Error())
			return err
		}

		if !Check2Orders(cfg.Options.IndexTableRows){
			err := fmt.Errorf("IndexTableRows(N) not suitable")
			fmt.Println("[error] ",err.Error())
			return err
		}
	}

	yt, err := ytfs.OpenInit(util.GetYTFSPath(), cfg.Options)
	if err != nil {
		return err
	}
	l := yt.PosIdx()
	if l < 5 {
		log.Println("[diskHash] ytfs_len:",l)
		err := diskHash.RandWrite(yt, uint(l))
		if err != nil {
			log.Println("[diskHash] randWrite to ytfs error:", err)
			return fmt.Errorf("write ytfs checkData error")
		}
	}
	//defer yt.Close()
	fmt.Println("YTFS init success")
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
	if runtime.GOOS == "linux" {
		cmd := exec.Command("bash", "-c", "ulimit -n 60000")
		stdin, _ := cmd.StdinPipe()
		fmt.Fprint(stdin, `
echo "* soft nofile 655350" > /etc/security/limits.conf
echo "* hard nofile 655350" >> /etc/security/limits.conf
echo "* soft nproc 655350" >> /etc/security/limits.conf
echo "* hard nproc 655350" >> /etc/security/limits.conf
echo "* soft core unlimited" >> /etc/security/limits.conf
echo "* hard core unlimited" >> /etc/security/limits.conf
`)
		cmd.Output()
	}

	ctx := context.Background()
	sn := instance.GetStorageNode()
	if nil == sn {
		log.Println("[init] GetStorageNode error: sn is nil")
		return
	}

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

func downloadYTDaemon() error {
	resp, err := http.Get(fmt.Sprintf("https://gengwenjuan.oss-cn-beijing.aliyuncs.com/ytfs-daemon-%s", runtime.GOOS))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), "ytfs-daemon"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer fl.Close()

	_, err = io.Copy(fl, resp.Body)

	return err
}

func reboot(pid int) {
	buf := bytes.NewBuffer([]byte{})

	if err := downloadYTDaemon(); err == nil {
		fmt.Fprintf(buf, "kill -9 %d;kill -9 %d;%s -d &", os.Getpid(), pid, path.Join(util.GetYTFSPath(), "ytfs-daemon"))
	} else {
		fmt.Fprintf(buf, "kill -9 %d;kill -9 %d;%s daemon -d &", os.Getpid(), pid, os.Args[0])
	}

	rebootShell := buf.String()
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
