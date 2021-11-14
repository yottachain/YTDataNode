package capProof

import (
	Ytfs "github.com/yottachain/YTFS"
)

func GetCapProofSpace(ytfs *Ytfs.YTFS) (realCap uint32) {
	availableShards := ytfs.GetCapProofSpace()
	if (availableShards % 65536) != 0 {
		realCap = (availableShards / 65536 + 1) * 65536
	}else {
		realCap = availableShards
	}
	return
}
