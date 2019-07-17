package daemon

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
)

func InitConfig() {
	viper.SetDefault("version", 3)
	viper.SetDefault("updateURL", "http://39.97.41.155/ytnode-update-config/update.yaml")

	viper.SetConfigName("daemon")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$PWD/")
	viper.AddConfigPath("$ytfs_path/")
	viper.AddConfigPath("$HOME/YTFS/")
	if err := viper.ReadInConfig(); err != nil {
		log.Println("读取配置文件失败：", err)

		if err := viper.SafeWriteConfig(); err != nil {
			log.Println("写入配置文件失败", err)
		}
	}
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Println("重新加载配置文件")
	})
	viper.SafeWriteConfig()
}

type UpdateConfig struct {
	RemoteVersion int32  `yaml:"remote_version"`
	DownloadURL   string `yaml:"donwload_url"`
}

func GetUpdateConfig() (*UpdateConfig, error) {
	var cfg UpdateConfig

	updateURL := viper.GetString("updateURL")
	resp, err := http.Get(updateURL)
	if err != nil {
		return nil, fmt.Errorf("获取升级配置失败：%s\n", err)
	}
	cfgbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("获取升级配置失败：%s\n", err)
	}
	err = yaml.Unmarshal(cfgbuf, &cfg)
	if err != nil {
		return nil, fmt.Errorf("获取升级配置失败：%s\n", err)
	}
	return &cfg, nil
}
