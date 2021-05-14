package actuator

import (
	"crypto/rand"

	lrcpkg "github.com/yottachain/YTLRC"
)

type ShardData [16384]byte

var TestData [164]lrcpkg.Shard

func EncodeForRecover() {
	var ori lrcpkg.OriginalShards
	var shdinfo lrcpkg.Shardsinfo
	shdinfo.LRCinit(13)

	originShards := originShardGen()
	ori.OriginalShards = originShards
	recdata := shdinfo.LRCEncode(originShards)
	for i := 0; i < 128; i++ {
		TestData[i] = originShards[i]
	}

	for k := 0; k < 36; k++ {
		TestData[k+128] = recdata[k]
	}
}

func originShardGen() []lrcpkg.Shard {
	dataArray := make([]lrcpkg.Shard, 128)

	for i := 0; i < 128; i++ {
		dataArray[i][0] = byte(i)
		_, err := rand.Read(dataArray[i][1:])
		if err != nil {
			panic(err)
		}
	}
	return dataArray
}

func init() {
	EncodeForRecover()
}
