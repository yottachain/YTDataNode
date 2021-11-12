package repoCmd

import (
	"fmt"
	"github.com/yottachain/YTDataNode/logger"
	"os"
	"path"

	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/storage"
)

var RepoCmd = &cobra.Command{
	Use:   "repo",
	Short: "YTFS 仓库操作",
}

var rebuildCmd = &cobra.Command{
	Use:   "rebuild",
	Short: "重建YTFS仓库",
	Run: func(cmd *cobra.Command, args []string) {
		var GB uint64 = 1 << 30
		var size uint64 = 4096
		var mc byte = 14
		fmt.Println("请输入矿机存储空间大小GB例如4T=4096:")
		_, err := fmt.Scanf("%d\n", &size)
		if err != nil {
			log.Println(err)
		}
	getMC:
		fmt.Println("请输入存储分组大小:可能取值8～20间2的整数倍值")
		_, err = fmt.Scanf("%d\n", &mc)
		if err != nil {
			log.Println(err)
			goto getMC
		}
		if mc < 8 || mc > 20 {
			fmt.Println("请输入范围8～20的数")
			goto getMC
		}
		cfg, err := config.ReadConfig()
		if err != nil {
			panic(err)
		}
		newCfg := cfg.ResetYTFSOptions(config.GetYTFSOptionsByParams(size*GB, 1<<mc))
		err = backData(cfg)
		if err != nil {
			if err == os.ErrExist {
				log.Println("文件夹已存在", err)
			} else {
				panic(err)
			}
		}
		err = copyData(cfg, &newCfg)
		if err != nil {
			log.Println("重建仓库失败，请从back目录恢复仓库")
			panic(err)
		}
		newCfg.Save()
		log.Println("YTFS仓库重建完成")
	},
}

func backData(cfg *config.Config) error {
	backdir := path.Join(util.GetYTFSPath(), "_back")
	err := os.Mkdir(path.Join(util.GetYTFSPath(), "_back"), os.ModePerm)
	if err != nil && err != os.ErrExist {
		return err
	}
	os.Rename(path.Join(util.GetYTFSPath(), "index.db"), path.Join(backdir, "index.db"))
	os.Rename(path.Join(util.GetYTFSPath(), "config.json"), path.Join(backdir, "config.json"))
	for k, v := range cfg.Storages {
		newPath := path.Join(backdir, path.Base(v.StorageName))
		os.Rename(v.StorageName, newPath)
		cfg.Storages[k].StorageName = newPath
	}
	return nil
}

func copyData(oldCfg *config.Config, newCfg *config.Config) error {
	newYT, err := ytfs.Open(util.GetYTFSPath(), newCfg.Options, oldCfg.IndexID)
	if err != nil {
		return err
	}
	ti, err := storage.GetTableIterator(path.Join(util.GetYTFSPath(), "_back", "index.db"), oldCfg.Options)
	if err != nil {
		return err
	}
	yd, err := storage.OpenYottaDisk(&(oldCfg.Storages[0]),false)
	if err != nil {
		return err
	}
	for {
		table, err := ti.GetNoNilTable()
		if err != nil {
			if err.Error() == "table end" {
				return nil
			}
			return err
		}
		for k, v := range table {
			data, err := yd.ReadData(v)
			if err != nil {
				return err
			}
			newYT.Put(k, data)
			log.Println("恢复分片：", k)
		}
	}

}

func init() {
	RepoCmd.AddCommand(rebuildCmd)
}
