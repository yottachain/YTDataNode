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
}

// Config 配置
type Config struct {
	ID         string `json:"ID"`
	privKey    ci.PrivKey
	BPList     []peerInfo `json:"BPList"`
	YTFSConfig *ytfsOpts.Options
}

// DefaultYTFSOptions default config
func DefaultYTFSOptions() *ytfsOpts.Options {
	const GB = 1024 * 1024 * 1024
	yp := util.GetYTFSPath()
	config := &ytfsOpts.Options{
		YTFSTag: "ytfs default setting",
		Storages: []ytfsOpts.StorageOptions{
			{
				StorageName:   fmt.Sprintf("%s-storage-1", yp),
				StorageType:   0,
				ReadOnly:      false,
				SyncPeriod:    1,
				StorageVolume: GB * 10,
				DataBlockSize: 1 << 14,
			},
			{
				StorageName:   fmt.Sprintf("%s-storage-2", yp),
				StorageType:   0,
				ReadOnly:      false,
				SyncPeriod:    1,
				StorageVolume: GB * 10,
				DataBlockSize: 1 << 14,
			},
		},
		ReadOnly:       false,
		SyncPeriod:     1,
		IndexTableCols: 0,
		IndexTableRows: 1 << 13,
		DataBlockSize:  1 << 14, // Just save HashLen for test.
		TotalVolumn:    2 << 30, // 1G
	}
	return config
}

// NewConfig ..
func NewConfig() *Config {
	cfg := new(Config)
	cfg.YTFSConfig = DefaultYTFSOptions()
	cfg.privKey, _ = util.RandomIdentity()
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
