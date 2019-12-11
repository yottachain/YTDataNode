package node

import (
	"fmt"
	"github.com/yottachain/YTDataNode/logger"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yottachain/YTDataNode/util"

	"github.com/multiformats/go-multiaddr"

	"github.com/yottachain/YTDataNode/config"

	"github.com/yottachain/YTDataNode/host"
	. "github.com/yottachain/YTDataNode/runtimeStatus"
	. "github.com/yottachain/YTDataNode/storageNodeInterface"

	// "github.com/yottachain/P2PHost"
	ytfs "github.com/yottachain/YTFS"
)

// Service 服务接口
type Service interface {
	Service()
}

//// Owner 归属信息
//type Owner struct {
//	ID       string
//	BuySpace uint64
//	HDD      uint64
//}

// AddrsManager 地址管理器
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
		log.Println("get public ip fail")
	} else {
		pubip, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println("get public ip fail:", err)
		}
		port, ok := os.LookupEnv("nat_port")
		if ok == false {
			port = "9001"
		}
		addr := fmt.Sprintf("/ip4/%s/tcp/%s", pubip, port)
		addr = strings.Replace(addr, "\n", "", -1)
		pubma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Println("fomate public ip fail:", err, addr)
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
	host          *host.Host
	ytfs          *ytfs.YTFS
	config        *config.Config
	addrsmanager  *AddrsManager
	runtimeStatus RuntimeStatus
	owner         *Owner
}

func (sn *storageNode) Owner() *Owner {
	return sn.owner
}

func (sn *storageNode) Runtime() RuntimeStatus {
	return sn.runtimeStatus.Update()
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
	return sn.Config().GetBPIndex()
}
func (sn *storageNode) Addrs() []string {
	return sn.addrsmanager.GetAddStrings()
}

func (sn *storageNode) SendBPMsg(index int, data []byte) ([]byte, error) {
	bp := sn.config.BPList[index]
	sn.host.ConnectAddrStrings(bp.ID, bp.Addrs)
	res, err := sn.Host().SendMsg(bp.ID, "/node/0.0.2", data)
	return res, err
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
	sn.owner = new(Owner)
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
