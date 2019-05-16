package node

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
	"yottachain/ytfs-util"

	"github.com/multiformats/go-multiaddr"

	"github.com/yottachain/YTDataNode/config"

	"github.com/yottachain/YTDataNode/host"

	// "github.com/yottachain/P2PHost"

	"github.com/yottachain/YTFS"
)

// StorageNode 存储节点接口
type StorageNode interface {
	Addrs() []string
	Host() *host.Host
	YTFS() *ytfs.YTFS
	GetBP() int
	Service()
	Config() *config.Config
}

type AddrsManager struct {
	addrs      []multiaddr.Multiaddr
	updateTime time.Time
	ttl        time.Duration
	sn         StorageNode
}

// UpdateAddrs 更新地址列表
func (am *AddrsManager) UpdateAddrs() {
	am.addrs = am.sn.Host().Addrs()
	resp, err := http.Get("http://39.97.41.155/self-ip")
	if err != nil {
		fmt.Println("get public ip fail")
	} else {
		pubip, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("get public ip fail:", err)
		}
		addr := fmt.Sprintf("/ip4/%s/tcp/9001", pubip)
		addr = strings.Replace(addr, "\n", "", -1)
		pubma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			fmt.Println("fomate public ip fail:", err, addr)
		} else {
			am.addrs = append(am.addrs, pubma)
			am.updateTime = time.Now()
		}
		am.updateTime = time.Now()
	}
}

// GetAddrs 获取地址列表
func (am *AddrsManager) GetAddrs() []multiaddr.Multiaddr {
	if am.addrs == nil || am.ttl < time.Now().Sub(am.updateTime) {
		am.UpdateAddrs()
	}
	return am.addrs
}

// GetAddStrings 获取地址看列表字符串数组
func (am *AddrsManager) GetAddStrings() []string {
	addrs := am.GetAddrs()
	addrstrings := make([]string, len(addrs))
	for k, v := range addrs {
		addrstrings[k] = v.String()
	}
	return addrstrings
}

type storageNode struct {
	host         *host.Host
	ytfs         *ytfs.YTFS
	config       *config.Config
	addrsmanager *AddrsManager
}

func (sn *storageNode) Host() *host.Host {
	return sn.host
}
func (sn *storageNode) Config() *config.Config {
	return sn.config
}

func (sn *storageNode) YTFS() *ytfs.YTFS {
	return sn.ytfs
}

func (sn *storageNode) GetBP() int {
	id := sn.Host().ID().Pretty()
	bpnum := byte(len(sn.Config().BPList))
	bpindex := id[len(id)-1] % bpnum
	return int(bpindex)
}
func (sn *storageNode) Addrs() []string {
	return sn.addrsmanager.GetAddStrings()
}

// NewStorageNode 创建存储节点
func NewStorageNode(cfg *config.Config) (StorageNode, error) {
	// pkbytes, err := base58.Decode(pkstring)
	// if err != nil {
	// 	return nil, fmt.Errorf("Bad private key string")
	// }
	// pk, err := ci.UnmarshalSecp256k1PrivateKey(pkbytes[1:33])
	// if err != nil {
	// 	return nil, fmt.Errorf("Bad format of private key")
	// }

	sn := &storageNode{}
	sn.config = cfg
	sn.addrsmanager = &AddrsManager{
		nil,
		time.Now(),
		time.Second * 10,
		sn,
	}
	// h, err := host.NewHost(host.ListenAddrStrings("/ip4/0.0.0.0/tcp/9001"), pk)

	sn.host = host.NewP2PHost()
	sn.host.SetPrivKey(sn.config.PrivKey())
	yp := util.GetYTFSPath()
	ys, err := ytfs.Open(yp, cfg.Options)
	if err != nil {
		return nil, fmt.Errorf("YTFS storage init faile")
	}
	sn.ytfs = ys
	if err != nil {
		return nil, err
	}

	return sn, nil
}
