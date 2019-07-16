package daemon

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yottachain/YTDataNode/util"
	"log"
	"net/http"
	"os"
	"path"
)

func checkupdate() (bool, *UpdateConfig) {
	version := viper.GetInt32("version")
	updateCfg, err := GetUpdateConfig()
	if err != nil {
		log.Println(err)
		return false, nil
	}
	if version >= updateCfg.RemoteVersion {
		return false, nil
	} else {
		return true, updateCfg
	}
}

func Update() {
	if ok, cfg := checkupdate(); ok {
		log.Println("有新的版本：", cfg.RemoteVersion)
	} else {
		log.Println("无可用新版本")
	}
}

func install(downloadURL string) {
	http.Get(downloadURL)
	os.Create(path.Join(util.GetYTFSPath(), "ytfs-node.temp"))
}

var UpdateCMD = &cobra.Command{
	Use:   "update",
	Short: "检查更新",
	Run: func(cmd *cobra.Command, args []string) {
		Update()
	},
}
