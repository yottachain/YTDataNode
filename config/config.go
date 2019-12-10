package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
	"github.com/yottachain/YTDataNode/logger"
	"io/ioutil"
	"os"
	"path"
	"time"

	ci "github.com/libp2p/go-libp2p-crypto"
	"github.com/yottachain/YTDataNode/util"
	ytfsOpts "github.com/yottachain/YTFS/opt"
)

type peerInfo struct {
	ID    string   `json:"ID"`
	Addrs []string `json:"Addrs"`
}

// Config 配置
type Config struct {
	ID            string `json:"ID"`
	privKey       ci.PrivKey
	PubKey        string
	BPList        []peerInfo    `json:"BPList"`
	Adminacc      string        `json:"Adminacc"`
	Relay         bool          `json:"Relay"`
	ListenAddr    string        `json:"ListenAddr"`
	APIListen     string        `json:"APIListen"`
	IndexID       uint32        `json:"IndexID"`
	PoolID        string        `json:"PoolID"`
	MaxConn       int           `json:"MaxConn"`
	TokenInterval time.Duration `json:"TokenInterval"`
	*ytfsOpts.Options
	UpdateURL string `json:"update_url"`
}

// DefaultYTFSOptions default config
func DefaultYTFSOptions() *ytfsOpts.Options {
	yp := util.GetYTFSPath()
	opts := ytfsOpts.DefaultOptions()
	for index, storage := range opts.Storages {
		storage.StorageName = fmt.Sprintf("%s/storage-%d", yp, index)
		storage.StorageVolume = 2 << 40
		storage.DataBlockSize = 1 << 14
		opts.Storages[index] = storage
	}
	opts.DataBlockSize = 1 << 14
	opts.TotalVolumn = 2 << 41
	opts.IndexTableCols = 1 << 14
	opts.IndexTableRows = 1 << 28
	return opts
}

// GetYTFSOptionsByParams 通过参数生成YTFS配置
func GetYTFSOptionsByParams(size uint64, n uint32) *ytfsOpts.Options {
	yp := util.GetYTFSPath()
	var d uint32 = 1 << 14
	m := size / uint64(d) / uint64(n)
	opts := &ytfsOpts.Options{
		YTFSTag: "ytfs",
		Storages: []ytfsOpts.StorageOptions{
			{
				StorageName:   path.Join(yp, "storage"),
				StorageType:   0,
				ReadOnly:      false,
				SyncPeriod:    1,
				StorageVolume: size,
				DataBlockSize: 1 << 14,
			},
		},
		ReadOnly:       false,
		SyncPeriod:     1,
		IndexTableCols: uint32(m),
		IndexTableRows: uint32(n),
		DataBlockSize:  d,
		TotalVolumn:    size,
	}
	return opts
}

// GetYTFSOptionsByParams2 通过参数生成YTFS配置, 多storage配置
func GetYTFSOptionsByParams2(totalSize uint64, storageSize uint64, m uint32) *ytfsOpts.Options {
	yp := util.GetYTFSPath()
	n := totalSize / uint64(m)
	opts := &ytfsOpts.Options{
		YTFSTag: "ytfs",
		Storages: []ytfsOpts.StorageOptions{
			{
				StorageName:   path.Join(yp, "storage"),
				StorageType:   0,
				ReadOnly:      false,
				SyncPeriod:    1,
				StorageVolume: storageSize,
				DataBlockSize: 1 << 14,
			},
		},
		ReadOnly:       false,
		SyncPeriod:     1,
		IndexTableCols: m,
		IndexTableRows: uint32(n),
		DataBlockSize:  1 << 14,
		TotalVolumn:    totalSize,
	}
	return opts
}

// NewConfig ..
func NewConfig() *Config {
	cfg := NewConfigByYTFSOptions(DefaultYTFSOptions())
	return cfg
}

func NewConfigByYTFSOptions(opts *ytfsOpts.Options) *Config {
	cfg := new(Config)
	cfg.ListenAddr = "/ip4/0.0.0.0/tcp/9001"
	cfg.APIListen = ":9002"
	cfg.Options = opts
	cfg.privKey, cfg.PubKey, _ = util.RandomIdentity2()

	cfg.Relay = true
	//cfg.BPList = getBPList()
	return cfg
}

func getBPList() []peerInfo {
	var bplist []peerInfo
	//var bpconfigurl = "http://download.yottachain.io/config/bp-test.json"
	//if url, ok := os.LookupEnv("bp-config-url"); ok {
	//	bpconfigurl = url
	//}
	//
	//log.Println("bpconfigurl", bpconfigurl)
	//resp, err := http.Get(bpconfigurl)
	//if err != nil {
	//	log.Println("获取BPLIST失败")
	//	os.Exit(1)
	//}
	//buf, err := ioutil.ReadAll(resp.Body)
	//if err != nil {
	//	log.Println("获取BPLIST失败")
	//	os.Exit(1)
	//}
	jsdata := `
[
    {

      "ID": "16Uiu2HAmPqE8R8U7CHRsKrNw9aVjdxUrGUEWBvNPGsVbaCBxrLkt",
      "Addrs": ["/ip4/49.234.139.206/tcp/9999"]
    },
    {

      "ID": "16Uiu2HAmR7VVxjZ516FkXmjoeAaMQ5UBEtqR1xPDmFhLi1ivb5GP",
      "Addrs": ["/ip4/129.211.72.15/tcp/9999"]
    },
    {

      "ID": "16Uiu2HAm1h55uTrWXATukWgMwKdYEEdSbkUpCwfSc2fznfebNcJL",
      "Addrs": ["/ip4/122.152.203.189/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmCtczNYRxsUj4xi4jMGFsfxn1oKYsbSLAYNsJZkJJLgsZ",
      "Addrs": ["/ip4/212.129.153.253/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmSXfk5mgmZd2YmA5Aug23vCdUesRuVEeCHSFUDeX2GxWv",
      "Addrs": ["/ip4/49.235.52.30/tcp/9999"]
    }
  ]

`
	buf := bytes.NewBufferString(jsdata)
	json.Unmarshal(buf.Bytes(), &bplist)
	return bplist
}

// Save ..
func (cfg *Config) Save() error {
	yp := util.GetYTFSPath()
	if ok, err := util.PathExists(yp); ok != true || err != nil {
		os.Mkdir(yp, os.ModePerm)
	}

	cfgPath := util.GetConfigPath()

	keyBytes, err := cfg.privKey.Raw()
	if err != nil {
		log.Println("配置保存失败", err)
	}
	ioutil.WriteFile(fmt.Sprintf("%s/swarm.key", yp), keyBytes, os.ModePerm)
	peerID, err := util.IdFromPublicKey(cfg.PubKey)
	if err != nil {
		return err
	}
	cfg.ID = peerID.Pretty()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(cfgPath, data, os.ModePerm)
}

func (cfg *Config) ReloadBPList() {
	//cfg.BPList = getBPList()
	//cfg.Save()
}

// NewKey 创建新的key
func (cfg *Config) NewKey() error {
	cfg.privKey, _ = util.RandomIdentity()
	id, err := util.IdFromPublicKey(cfg.PubKey)
	if err != nil {
		return err
	}
	cfg.ID = id.Pretty()
	return nil
}

// GetBPIndex 返回bpindex
func (cfg *Config) GetBPIndex() int {
	id := cfg.IndexID
	bpnum := len(cfg.BPList)
	if bpnum == 0 {
		return 0
	}
	bpindex := id % uint32(bpnum)
	return int(bpindex)
}

// ReadConfig 读配置
func ReadConfig() (*Config, error) {
	var cfg Config
	data, err := ioutil.ReadFile(util.GetConfigPath())
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	keyBytes, err := ioutil.ReadFile(fmt.Sprintf("%s/swarm.key", util.GetYTFSPath()))
	if err != nil {
		return nil, err
	}
	privk, err := ci.UnmarshalSecp256k1PrivateKey(keyBytes)
	if err != nil {

		return nil, err
	}
	cfg.privKey = privk
	return &cfg, nil
}

// PrivKey ..
func (cfg *Config) PrivKey() ci.PrivKey {
	return cfg.privKey
}
func (cfg *Config) PrivKeyString() string {
	buf, _ := cfg.privKey.Raw()
	return base58.Encode(buf)
}

func (cfg *Config) Version() uint32 {
	return 12
}

func Version() uint32 {
	return new(Config).Version()
}

func (cfg Config) ResetYTFSOptions(opts *ytfsOpts.Options) Config {
	cfg.Options = opts
	return cfg
}
