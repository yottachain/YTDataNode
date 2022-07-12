package node

import (
	"context"
	"fmt"
	"github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/slicecompare"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yottachain/YTDataNode/util"

	"github.com/multiformats/go-multiaddr"

	"github.com/yottachain/YTDataNode/config"

	. "github.com/yottachain/YTDataNode/runtimeStatus"
	. "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTHost"
	. "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/option"

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
	addrsCmd     []multiaddr.Multiaddr
	updateTime time.Time
	ttl        time.Duration
	sn         StorageNode
}

// UpdateAddrs 更新地址列表
func (am *AddrsManager) UpdateAddrs() {
	var port string
	var portCmd string

	ls, lsCmd := am.sn.Host().Listenner()
	am.addrs, port = am.sn.Host().Addrs(ls)

	if lsCmd == nil {
		am.addrsCmd = am.addrs
		portCmd = port
	} else {
		am.addrsCmd, portCmd = am.sn.Host().Addrs(lsCmd)
	}

	var realIP string;

	url := "http://dnapi.yottachain.net/self-ip"
	resp, err := http.Get(url)
	if err != nil {
		if ip, ok := os.LookupEnv("local_host_ip"); ok && ip != "" {
			realIP = ip
		} else {
			log.Printf("get public ip fail, url:%s\n", url)
			return
		}
	} else {
		defer resp.Body.Close()

		if ip, ok := os.LookupEnv("local_host_ip"); ok && ip != "" {
			realIP = ip
		} else {
			pubip, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Println("get public ip fail:", err)
				return
			}
			realIP = string(pubip)
		}
	}

	pt, ok1 := os.LookupEnv("nat_port")
	ptCmd, ok2 := os.LookupEnv("nat_port_1")
	if ok1 {
		port = pt
		addr := fmt.Sprintf("/ip4/%s/tcp/%s", realIP, port)
		addr = strings.Replace(addr, "\n", "", -1)
		pubma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Println("format public ip fail:", err, addr)
		} else {
			am.addrs = append(am.addrs, pubma)
		}
	}

	if ok2 {
		portCmd = ptCmd
		addr := fmt.Sprintf("/ip4/%s/tcp/%s", realIP, portCmd)
		addr = strings.Replace(addr, "\n", "", -1)
		pubma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Println("format public ip fail:", err, addr)
		} else {
			am.addrsCmd = append(am.addrsCmd, pubma)
		}
	}

	am.updateTime = time.Now()
}

// GetAddrs 获取地址列表
func (am *AddrsManager) GetAddrs() ([]multiaddr.Multiaddr, []multiaddr.Multiaddr) {
	if am.addrs == nil || am.ttl < time.Now().Sub(am.updateTime) {
		am.UpdateAddrs()
	}
	return am.addrs, am.addrsCmd
}

// GetAddStrings 获取地址看列表字符串数组
func (am *AddrsManager) GetAddStrings() ([]string, []string) {
	addr, addrCmd := am.GetAddrs()
	addrs := make([]string, len(addr))
	for k, v := range addr {
		addrs[k] = v.String()
	}

	addrsCmd := make([]string, len(addrCmd))
	for k, v := range addrCmd {
		addrsCmd[k] = v.String()
	}
	return addrs, addrsCmd
}

type storageNode struct {
	host          Host
	ytfs          *ytfs.YTFS
	config        *config.Config
	addrsmanager  *AddrsManager
	runtimeStatus RuntimeStatus
	owner         *Owner
	TmpDB        *CompDB
}

func (sn *storageNode) Owner() *Owner {
	return sn.owner
}

func (sn *storageNode) Runtime() RuntimeStatus {
	return sn.runtimeStatus.Update()
}

func (sn *storageNode) Host() Host {
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
func (sn *storageNode) Addrs() ([]string, []string){
	return sn.addrsmanager.GetAddStrings()
}

func (sn *storageNode) GetCompareDb() *CompDB{
	return sn.TmpDB
}

func (sn *storageNode) SendBPMsg(index int, id int32, data []byte) ([]byte, error) {
	bp := sn.config.BPList[index]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clt, err := sn.host.ClientStore().GetByAddrString(ctx, bp.ID, bp.Addrs)
	if err != nil {
		return nil, err
	}
	res, err := clt.SendMsg(ctx, id, data)
	return res, err
}

// NewStorageNode 创建存储节点
func NewStorageNode(cfg *config.Config) (StorageNode, error) {
	sn := &storageNode{}
	sn.config = cfg
	sn.owner = new(Owner)
	sn.addrsmanager = &AddrsManager{
		nil,
		nil,
		time.Now(),
		time.Minute * 60,
		sn,
	}

	ma, err := multiaddr.NewMultiaddr(cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("ListenAddr: %s", err.Error())
	}

	var maCmd multiaddr.Multiaddr

	if cfg.ListenAddrCmd != "" && cfg.ListenAddr != cfg.ListenAddrCmd {
		maCmd, err = multiaddr.NewMultiaddr(cfg.ListenAddrCmd)
		if err != nil {
			return nil, fmt.Errorf("ListenAddrCmd: %s", err.Error())
		}
	}


	hst, err := host.NewHost(option.Identity(sn.config.PrivKey()),
					option.ListenAddr(ma),
					option.ListenAddrCmd(maCmd),
					option.OpenPProf(":10000"),
					option.Version(int32(cfg.Version())))
	if err != nil {
		log.Printf("host new fail! err:%s\n", err.Error())
		return nil, err
	}
	sn.host = hst

	yp := util.GetYTFSPath()
	ys, err := ytfs.Open(yp, cfg.Options, cfg.IndexID)
	if err != nil {
		log.Println(err.Error())
		return nil , fmt.Errorf("YTFS storage init failed, err:%s", err.Error())
	}
	sn.ytfs = ys

	sn.TmpDB, err = slicecompare.OpenTmpRocksDB(slicecompare.Comparedb)
	if err != nil{
		log.Println("[slicecompare] open compare_db error")
		return nil , fmt.Errorf("open compare_db error:%s", err.Error())
	}
	return sn, nil
}
