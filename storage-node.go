package node

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yottachain/YTDataNode/util"

	"github.com/shirou/gopsutil/mem"

	"github.com/multiformats/go-multiaddr"
	"github.com/shirou/gopsutil/cpu"

	"github.com/yottachain/YTDataNode/config"

	"github.com/yottachain/YTDataNode/host"

	// "github.com/yottachain/P2PHost"

	ytfs "github.com/yottachain/YTFS"
)

// StorageNode 存储节点接口
type StorageNode interface {
	Addrs() []string
	Host() *host.Host
	YTFS() *ytfs.YTFS
	GetBP() int
	Service()
	Config() *config.Config
	Runtime() RuntimeStatus
	Owner() *Owner
}

// Owner 归属信息
type Owner struct {
	ID       string
	BuySpace uint64
	HDD      uint64
}

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
		fmt.Println("get public ip fail")
	} else {
		pubip, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("get public ip fail:", err)
		}
		port, ok := os.LookupEnv("nat_port")
		if ok == false {
			port = "9001"
		}
		fmt.Printf("[debug %s]: env: %s ok %b", time.Now().String(), port, ok)
		addr := fmt.Sprintf("/ip4/%s/tcp/%s", pubip, port)
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

// RuntimeStatus 运行时状态
type RuntimeStatus struct {
	CPU   []uint32
	AvCPU uint32
	Mem   uint32
}

// NewRuntimeStatus 创建状态管理器
func NewRuntimeStatus() *RuntimeStatus {
	return new(RuntimeStatus)
}

// Update 刷新
func (rs *RuntimeStatus) Update() RuntimeStatus {
	cpupercent, _ := cpu.Percent(0, true)
	var cpupint32 = make([]uint32, len(cpupercent))
	var sumcpu uint32
	for k, v := range cpupercent {
		cpupint32[k] = uint32(v)
		sumcpu = sumcpu + cpupint32[k]
	}
	m, _ := mem.VirtualMemory()
	rs.Mem = uint32(m.UsedPercent)
	rs.CPU = cpupint32
	rs.AvCPU = sumcpu / uint32(len(cpupercent))
	fmt.Println("cupsum:", sumcpu, len(cpupercent))
	return *rs
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
