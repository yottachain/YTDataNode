package capProof

import (
	"sync/atomic"
	"time"

	Ytfs "github.com/yottachain/YTFS"
)

var AvailableShards uint32

func TimerRun(ytfs *Ytfs.YTFS){
	for {
		//AvailableShards = getCapProofSpace(ytfs)
		AvailableShards = 8388608
		<-time.After(time.Minute*10)
	}
}

func getCapProofSpace(ytfs *Ytfs.YTFS) (realCap uint32) {
	availableShards := ytfs.GetCapProofSpace()
	if (availableShards % 65536) != 0 {
		realCap = (availableShards / 65536 + 1) * 65536
	}else {
		realCap = availableShards
	}
	return
}

func GetCapProofSpace(ytfs *Ytfs.YTFS) (realCap uint32) {
	if atomic.LoadUint32(&AvailableShards) == 0 {
		//AvailableShards = getCapProofSpace(ytfs)
		AvailableShards = 8388608
	}
	return  AvailableShards
}
