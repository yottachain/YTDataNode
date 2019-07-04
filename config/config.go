package config

import (
	"encoding/json"
	"fmt"
	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
	"io/ioutil"
	"log"
	"net/http"
	"os"

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
	PubKey     string
	BPList     []peerInfo `json:"BPList"`
	Adminacc   string     `json:"Adminacc"`
	Relay      bool       `json:"Relay"`
	ListenAddr string     `json:"ListenAddr"`
	APIListen  string     `json:"APIListen"`
	IndexID    uint32     `json:"IndexID"`
	PoolID     string     `json:"PoolID"`
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
	cfg.privKey, cfg.PubKey, _ = util.RandomIdentity2()

	cfg.Relay = true
	cfg.BPList = getBPList()
	return cfg
}

func getBPList() []peerInfo {
	var bplist []peerInfo
	resp, err := http.Get("http://download.yottachain.io/config/bp.json")
	if err != nil {
		log.Println("获取BPLIST失败")
		os.Exit(1)
	}
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("获取BPLIST失败")
		os.Exit(1)
	}
	json.Unmarshal(buf, &bplist)
	return bplist
}

// Save ..
func (cfg *Config) Save() error {
	yp := util.GetYTFSPath()
	if ok, err := util.PathExists(yp); ok != true || err != nil {
		os.Mkdir(yp, os.ModePerm)
	}

	cfgPath := util.GetConfigPath()
	keyBytes, _ := cfg.privKey.Raw()
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

//func (cfg *Config) PubKeyString() string {
//	key, err := util.GetPublicKey(cfg.PrivKeyString())
//	if err != nil {
//		log.Println(err, cfg.PrivKeyString())
//	}
//	return key
//}
