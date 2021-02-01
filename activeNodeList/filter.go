package activeNodeList

import "strings"

type FilterFunc func(data *Data) bool

func Filter(datas []*Data, filter FilterFunc) []*Data {
	var res = make([]*Data, 0)
	for k, _ := range datas {
		if filter(datas[k]) {
			res = append(res, datas[k])
		}
	}

	return res
}

func NewNoAddrFilter(addrs string) FilterFunc {
	return func(data *Data) bool {
		for _, v := range data.IP {
			if strings.Contains(v, addrs) {
				return false
			}
		}
		return true
	}
}
