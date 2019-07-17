package daemon

import (
	"github.com/inconshreveable/go-update"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yottachain/YTDataNode/util"
	"log"
	"net/http"
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
		if err := doUpdate(cfg.DownloadURL); err != nil {
			log.Fatal(err, cfg.DownloadURL, cfg)
		}
	} else {
		log.Println("无可用新版本")
	}
}

func doUpdate(downloadURL string) error {
	resp, err := http.Get(downloadURL + "-darwin")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	err = update.Apply(resp.Body, update.Options{
		OldSavePath: util.GetYTFSPath() + "/ytfs-node.bak",
		//TargetPath:  util.GetYTFSPath() + "/ytfs-node",
	})
	if err != nil {
		return err
	}
	return nil
}

var UpdateCMD = &cobra.Command{
	Use:   "update",
	Short: "检查更新",
	Run: func(cmd *cobra.Command, args []string) {
		Update()
	},
}
