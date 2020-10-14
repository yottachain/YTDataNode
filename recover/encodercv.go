package recover

import(
	//"math/rand"
	lrcpkg "github.com/yottachain/YTLRC"
	"crypto/rand"
)

type Shard [16384]byte

func (re *RecoverEngine)EncodeForRecover(){
	var ori lrcpkg.OriginalShards
	var shdinfo lrcpkg.Shardsinfo
	shdinfo.LRCinit(13)

	originShards := originShardGen()
	ori.OriginalShards = originShards
	recdata := shdinfo.LRCEncode(originShards)
    for i := 0; i < 128; i++{
    	re.tstdata[i] = originShards[i]
	}

	for k := 0; k < 36; k++{
		re.tstdata[k+128] = recdata[k]
	}
}

func originShardGen() []lrcpkg.Shard{
	dataArray := make([]lrcpkg.Shard,128)

	for i := 0; i < 128; i++ {
		dataArray[i][0] = byte(i)
		_, err := rand.Read(dataArray[i][1:])
		if err !=nil{
			panic(err)
		}
	}
	return dataArray
}

func checkOnline(){

}
