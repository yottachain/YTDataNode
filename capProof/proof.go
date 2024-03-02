package capProof

import (
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	Ytfs "github.com/yottachain/YTFS"
	"sync/atomic"
	"time"
)

var AvailableShards uint32

func TimerRun(ytfs *Ytfs.YTFS) {
	for {
		err := ytfs.NewCapProofCheckInit()
		if err == nil {
			break
		} else {
			fmt.Printf("cap proof chech init error:%s\n", err.Error())
			<-time.After(time.Millisecond * 1)
		}
	}

	for {
		//AvailableShards = getCapProofSpace(ytfs)
		//<-time.After(time.Minute*10)
		_ = ytfs.NewCapProofDataFill()
		<-time.After(time.Millisecond * 1)
	}
}

func getCapProofSpace(ytfs *Ytfs.YTFS) (realCap uint32) {
	if config.Gconfig.NeedCapProof {
		availableShards := ytfs.GetCapProofSpace()
		if (availableShards % 65536) != 0 {
			realCap = (availableShards/65536 + 1) * 65536
		} else {
			realCap = availableShards
		}
	} else {
		realCap = 8388608
	}
	return
}

func GetCapProofSpace(ytfs *Ytfs.YTFS) (realCap uint32) {
	if atomic.LoadUint32(&AvailableShards) == 0 {
		AvailableShards = getCapProofSpace(ytfs)
	}
	return AvailableShards
}
