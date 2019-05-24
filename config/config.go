package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/libp2p/go-libp2p-peer"

	ci "github.com/libp2p/go-libp2p-crypto"
	"github.com/yottachain/YTDataNode/util"
	ytfsOpts "github.com/yottachain/YTFS/opt"
)

type peerInfo struct {
	ID    string
	Addrs []string
}

// Config 配置
type Config struct {
	ID         string `json:"ID"`
	privKey    ci.PrivKey
	BPList     []peerInfo `json:"BPList"`
	ListenAddr string     `json:"ListenAddr"`
	APIListen  string     `json:"APIListen"`
	IndexID    uint32     `json:"IndexID"`
	*ytfsOpts.Options
}

// DefaultYTFSOptions default config
func DefaultYTFSOptions() *ytfsOpts.Options {
	yp := util.GetYTFSPath()
	opts := ytfsOpts.DefaultOptions()
	for index, storage := range opts.Storages {
		storage.StorageName = fmt.Sprintf("%s/storage-%d", yp, index)
		storage.StorageVolume = (2 << 30) * 10
		storage.DataBlockSize = 1 << 14
		opts.Storages[index] = storage
	}
	opts.DataBlockSize = 1 << 14
	opts.TotalVolumn = (2 << 30) * 10 * 2
	return opts
}

// NewConfig ..
func NewConfig() *Config {
	cfg := new(Config)
	cfg.ListenAddr = "/ip4/0.0.0.0/tcp/9001"
	cfg.APIListen = ":9002"
	cfg.Options = DefaultYTFSOptions()
	cfg.privKey, _ = util.RandomIdentity()
	cfg.BPList = []peerInfo{
		peerInfo{
			"16Uiu2HAkyHhwuzkR6fRhKbhUBVMySBKKtLCRkReYTJQEyfCkPSfN",
			[]string{
				"/ip4/152.136.16.118/tcp/9999",
			},
		},
		peerInfo{
			"16Uiu2HAm9fBJNUzSD5V9aFJQQHbxE3rPsTiyrYk7vju18JCf3xm8",
			[]string{
				"/ip4/152.136.17.115/tcp/9999",
			},
		},
		peerInfo{
			"16Uiu2HAkwNCD9HSH5hh36LmzgLjRcQiQFpT9spwspaAM5AH3rqA9",
			[]string{
				"/ip4/152.136.18.185/tcp/9999",
			},
		},
	}
	return cfg
}

// Save ..
func (cfg *Config) Save() error {
	yp := util.GetYTFSPath()
	if ok, err := util.PathExists(yp); ok != true || err != nil {
		os.Mkdir(yp, os.ModePerm)
	}

	cfgPath := util.GetConfigPath()
	keyBytes, _ := cfg.privKey.Bytes()
	ioutil.WriteFile(fmt.Sprintf("%s/swarm.key", yp), keyBytes, os.ModePerm)
	peerID, err := peer.IDFromPrivateKey(cfg.privKey)
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
	privk, err := ci.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		fmt.Println(len(keyBytes))
		return nil, err
	}
	cfg.privKey = privk
	return &cfg, nil
}

// PrivKey ..
func (cfg *Config) PrivKey() ci.PrivKey {
	return cfg.privKey
}
