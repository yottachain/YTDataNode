package test

import (
	"crypto/sha256"
	"path"
	"testing"

	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
)

func TestAddShard(t *testing.T) {
	cfg, err := config.ReadConfig()
	ytfs, err := ytfs.Open(util.GetYTFSPath(), cfg.Options)
	data := []byte("3212testdata22132")
	hash := sha256.New()

	var key [32]byte
	copy(key[:], hash.Sum(data))
	ytfs.Put(common.IndexTableKey(key), data)
	if err != nil {
		t.Error(err, cfg)
	}
}

func TestTableIterator(t *testing.T) {
	opts, err := opt.ParseConfig(util.GetConfigPath())
	yd, err := storage.OpenYottaDisk(&opts.Storages[0])
	if err != nil {
		t.Error(err)
	}
	ti, err := storage.GetTableIterator(path.Join(util.GetYTFSPath(), "index.db"), opts)
	if err != nil {
		t.Error(err)
	}
	for {
		table, err := ti.GetNoNilTable()
		if err != nil {
			t.Log(err)
			break
		}
		for k, v := range table {
			t.Log(k, v)
			buf, err := yd.ReadData(v)
			if err != nil {
				t.Error(err)
			}
			t.Log(string(buf))
		}
	}

}
