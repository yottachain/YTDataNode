package registerCmd

import "fmt"

// 注册矿机用的表单
type RegForm struct {
	BaseUrl      string   `yaml:"BaseUrl"`
	MinerId      int      `yaml:"MinerId"`   // 矿机ID
	AdminAcc     string   `yaml:"AdminAcc"`  //管理账户
	DepAcc       string   `yaml:"DepAcc"`    //抵押账户
	DepAccKey    string   `yaml:"DepAccKey"` //抵押账户私钥
	PoolId       string   `yaml:"PoolId"`    //所要加入的矿池id
	PoolOwner    string   `yaml:"PoolOwner"` //矿池管理员账号
	PoolOwnerKey string   `yaml:"PoolOwnerKey"`
	MinerOwner   string   `yaml:"MinerOwner"` //指定的矿机收益账户
	MaxSpace     uint64   `yaml:"MaxSpace"`   //矿机的最大可采购空间 (65536的整数倍，以分片为单位)
	DepAmount    uint64   `yaml:"DepAmount"`  //为矿机缴纳的押金数量
	IsCalc       bool     `yaml:"IsCalc"`     //是否让系统自动计算押金，如果指定为
	BPList       []string `yaml:"BPList"`     //BP服务器地址列表
}

func GetNewMinerIDUrl(url string) string {
	return fmt.Sprintf("http://%s/newnodeid", url)
}
func GetRegisterUrl(url string) string {
	return fmt.Sprintf("http://%s/preregnode", url)
}
