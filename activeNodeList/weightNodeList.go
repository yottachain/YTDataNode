package activeNodeList

import (
	"github.com/yottachain/YTDataNode/util"
	"time"
)

type WeightNodeList struct {
	*util.DataCache
}

func NewWeightNodeList(ttl time.Duration, updateTime time.Duration, groupSize int, noIP []string) *WeightNodeList {
	wl := new(WeightNodeList)
	wl.DataCache = util.NewDataCache(ttl, func() interface{} {
		nodeList := GetNodeListByTimeAndGroupSize(updateTime, groupSize)
		nodeList = GetWeightNodeList(nodeList)
		nodeList = GetNoIPNodeList(nodeList, noIP)
		return nodeList
	})
	return wl
}
func (wl *WeightNodeList) Get() []*Data {
	res := wl.DataCache.Get()
	if res == nil {
		return nil
	}
	return res.([]*Data)
}
