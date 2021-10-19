package capProof

import (
	Ytfs "github.com/yottachain/YTFS"
)

func GetCapProof(ytfs *Ytfs.YTFS) bool {
	//抵押空间大于配置空间 属于容量作弊？
	//抵押空间大于实际空间 属于容量作弊？
	totalConfCap, totalRealCap, _ := ytfs.GetTotalCap()

	//链上空间大于配置空间认为是容量作弊

	if totalConfCap > totalRealCap {
		return true
	}

	return false
}
