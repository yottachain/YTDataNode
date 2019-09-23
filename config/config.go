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
	cfg.BPList = getBPList()
	return cfg
}

func getBPList() []peerInfo {
	var bplist []peerInfo
	//var bpconfigurl = "http://download.yottachain.io/config/bp.json"
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
      "ID": "16Uiu2HAmTT47W5sBd2oF6MMwft8GAqDyZJFXc2HVit3oQ8p4jijh",
      "Addrs": ["/ip4/106.52.13.73/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm3Rea9AGSJYZLgU3ZqdzNoZfb5UUmYF8SAN7FH97HNauq",
      "Addrs": ["/ip4/129.28.188.167/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmUhi4iTzvqoJZUJeomnemhWVAf8Vmy25nrPA7HzpAEbm9",
      "Addrs": ["/ip4/139.155.106.14/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmNi1Ht6AFHRCSNeBja5ej6WNVNf3ghFKPdNJ9uDsApXvT",
      "Addrs": ["/ip4/39.105.24.143/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmLXRAj6ntKgzAQ8PwHqUmqHUhhRRekHknzwPHoK4HEHuX",
      "Addrs": ["/ip4/47.104.13.155/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm2JAGMgSmHmwg5UA7L6RpqTm1F5Dc6xVqyDjY7HM73TLB",
      "Addrs": ["/ip4/106.15.235.83/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmE83eDy4ypdMokJmdecUFJKPB3iCEQbu5qeR36V2sCiwF",
      "Addrs": ["/ip4/118.190.38.93/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm3mTmdKt48HFhkjZ2qqUTyBH79xkW4TFfPwMzwnZiDcXT",
     "Addrs": ["/ip4/112.125.27.112/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmPcsnLY8fnU9qzN4hJrTtARJL2CrF3DXNK5uSEASLXXBJ",
      "Addrs": ["/ip4/129.28.191.41/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm75hpYM7SXomQaTWjoNU5xQrpUmm7naoUJEeWKKXq3Lei",
      "Addrs": ["/ip4/39.104.164.98/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmCT9SEfxzCaTUQnzuh7wQU5EQFR1dmtebwd7HjKWR3zj1",
      "Addrs": ["/ip4/47.112.150.170/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmKwHyu5xjkj8urA6UcssWyu93PowU5Uk83FehVBTv7JsR",
      "Addrs": ["/ip4/47.97.217.154/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmQbcFyWzRfiHjnsekzcvcFSPFXae8bUoa7F3YpjdwiSh8",
      "Addrs": ["/ip4/120.92.140.173/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmThsJVyAuz1cc6M33yKhe36WDfBocp9BfDTDGSDQync8G",
      "Addrs": ["/ip4/112.74.173.232/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm7MeyYNqupFB32axs3zzcGBwzHLAZeDCTRebaUnmXPAFe",
      "Addrs": ["/ip4/152.136.14.175/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmRd62XLp5bbCU1yhXkgv2N59NizPPBnaaGYWQsm2V1evM",
      "Addrs": ["/ip4/39.105.98.237/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmV6kpceDrpojkfi7qPVhrWpaSE9UjmznzszyMuRNxn7je",
      "Addrs": ["/ip4/39.98.54.152/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAmUPU1RR9ASUqwYaA8N9xyk6T8qY9wN4JbTBZH71cBZJm2",
      "Addrs": ["/ip4/39.104.201.48/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm8iV5UhQHRd2EXSjB1hCKUYwedVzbt4iUncuCJ99ddGNs",
      "Addrs": ["/ip4/47.112.138.94/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm8jAW9tfocDdsqEV83Vni1L4bfc7X4oDm1N5A9HHhjwj5",
      "Addrs": ["/ip4/47.106.202.196/tcp/9999"]
    },
    {
      "ID": "16Uiu2HAm5B6Zgp91hzzdQywTU3xphKsXnnEbTD2UE61rxzm7Qwpc",
      "Addrs": ["/ip4/62.234.163.238/tcp/9999"]
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
	cfg.BPList = getBPList()
	cfg.Save()
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
	buf, _ := cfg.privKey.Bytes()
	return base58.Encode(buf)
}

func (cfg *Config) Version() uint32 {
	return 9
}

func Version() uint32 {
	return new(Config).Version()
}

func (cfg Config) ResetYTFSOptions(opts *ytfsOpts.Options) Config {
	cfg.Options = opts
	return cfg
}
