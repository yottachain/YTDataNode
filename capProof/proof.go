package capProof

import (
	Ytfs "github.com/yottachain/YTFS"
)

func GetCapProofSpace(ytfs *Ytfs.YTFS) uint32 {
	return ytfs.GetCapProofSpace()
}
