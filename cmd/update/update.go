package update

import (
	"fmt"
	"github.com/inconshreveable/go-update"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"
	"github.com/yottachain/YTDataNode/config"
	"path"
	//"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTDataNode/logger"
	"net/http"
	"os"
	"runtime"
)

var force bool

type UpdateConfig struct {
	RemoteVersion int32  `yaml:"remote_version"`
	DownloadURL   string `yaml:"download_url"`
}

func getUpdateConfig() (*UpdateConfig, error) {
	var cfg UpdateConfig
	var updateURL = "http://39.97.41.155/ytnode-update-config/update.yaml"
	if url, ok := os.LookupEnv("update_url"); ok {
		updateURL = url
	}
	resp, err := http.Get(updateURL)
	if err != nil {
		return nil, fmt.Errorf("读取配置失败%s\n", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("获取更新配置失败:%d\n", resp.StatusCode)
	}
	viper.SetConfigType("yaml")
	err = viper.ReadConfig(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取配置失败%s\n", err)
	}

	cfg.RemoteVersion = viper.GetInt32(fmt.Sprintf("%s.%s.remote_version", runtime.GOARCH, runtime.GOOS))
	cfg.DownloadURL = viper.GetString(fmt.Sprintf("%s.%s.download_url", runtime.GOARCH, runtime.GOOS))
	log.Println(cfg.RemoteVersion, cfg.DownloadURL)

	return &cfg, nil
}

func CheckUpdate(cfg *UpdateConfig) bool {
	currVersion := config.Version()
	if cfg.RemoteVersion > int32(currVersion) {
		return true
	}
	return false
}

func doUpdate(downloadURL string) error {
	log.Println("正在更新")
	resp, err := http.Get(downloadURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	err = update.Apply(resp.Body, update.Options{
		TargetPath:  os.Args[0],
		OldSavePath: path.Join(path.Dir(os.Args[0]), "ytfs-node.old"),
	})
	if err != nil {
		return err
	}
	log.Println("更新完成")
	return nil
}

func Update() error {
	cfg, err := getUpdateConfig()
	if err != nil {
		return err
	}
	if CheckUpdate(cfg) || force {
		log.Println("即将更新版本：", cfg.RemoteVersion)
		return doUpdate(cfg.DownloadURL)
	} else {
		return fmt.Errorf("当前版本已是最新版本")
	}
}

func UpdateForce() error {
	cfg, err := getUpdateConfig()
	if err != nil {
		return err
	}
	log.Println("即将更新版本：", cfg.RemoteVersion)
	return doUpdate(cfg.DownloadURL)
}

var UpdateCMD = &cobra.Command{
	Use:   "update",
	Short: "检查更新",
	Run: func(cmd *cobra.Command, args []string) {
		err := Update()
		if err != nil {
			log.Println("更新失败:", err)
			log.Println("使用 update -f 强制更新")
		}
	},
}

func init() {
	UpdateCMD.Flags().BoolVarP(&force, "force", "f", false, "是否强制更新")
}
