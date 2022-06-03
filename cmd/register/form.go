package registerCmd

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

// 注册矿机用的表单
type RegForm struct {
	BaseUrl      string   `yaml:"BaseUrl"`
	AdminAcc     string   `yaml:"AdminAcc"`     //管理账户
	DepAcc       string   `yaml:"DepAcc"`       //抵押账户
	DepAccKey    string   `yaml:"DepAccKey"`    //抵押账户私钥
	PoolId       string   `yaml:"PoolId"`       //所要加入的矿池id
	PoolOwner    string   `yaml:"PoolOwner"`    //矿池管理员账号
	PoolOwnerKey string   `yaml:"PoolOwnerKey"` //矿池管理员账号私钥
	MinerOwner   string   `yaml:"MinerOwner"`   //指定的矿机收益账户
	MaxSpace     uint64   `yaml:"MaxSpace"`     //矿机的最大可采购空间 (65536的整数倍，以分片为单位)
	DepAmount    uint64   `yaml:"DepAmount"`    //为矿机缴纳的押金数量
	IsCalc       bool     `yaml:"IsCalc"`       //是否让系统自动计算押金，如果指定为
	ISBlockDev   bool     `yaml:"IsBlockDev"`   //是否是块设备
	StoragePath  string   `yaml:"StoragePath"`  //存储设备路径，如果是块设备的话自动选择
	M            uint32   `yaml:"M"`            //分组大小，默认2048
	BPList       []string `yaml:"BPList"`       //BP服务器地址列表
}

func GetNewMinerIDUrl(url string) string {
	return fmt.Sprintf("http://%s/newnodeid", url)
}
func GetRegisterUrl(url string) string {
	return fmt.Sprintf("http://%s/preregnode", url)
}

func GetFormTemplate() string {
	var form = RegForm{}

	//for index := 0; index < 21; index++ {
	//	url := fmt.Sprintf("sn%02d.yottachain.net", index)
	//	form.BPList = append(form.BPList, fmt.Sprintf("%s:8082", url))
	//}

	form.BPList = append(form.BPList, "sn.yottachain.net:8082")
	form.BaseUrl = "http://dnapi1.yottachain.net:8888"
	form.M = 2048
	form.MaxSpace = 1024
	form.IsCalc = true
	form.DepAmount = 1024
	form.AdminAcc = "管理账户"
	form.DepAcc = "抵押账户"
	form.DepAccKey = "抵押账户私钥"
	form.PoolId = "所要加入的矿池id"
	form.PoolOwner = "矿池管理员账号"
	form.PoolOwnerKey = "矿池管理员账号私钥"
	form.MinerOwner = "指定的矿机收益账户"
	form.ISBlockDev = true
	form.StoragePath = "/dev/vdb"

	template, _ := yaml.Marshal(form)

	return string(template)
}
