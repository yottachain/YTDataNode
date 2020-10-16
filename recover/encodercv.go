package recover

import (
	"crypto/rand"
	"github.com/yottachain/YTDataNode/activeNodeList"
	log "github.com/yottachain/YTDataNode/logger"
	//"math/rand"
	"github.com/yottachain/YTDataNode/message"
	lrcpkg "github.com/yottachain/YTLRC"
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


func (re *RecoverEngine)RecoverTst(td message.TaskDescription, lrch *LRCHandler) (bool, error){
	defer lrch.si.FreeHandle()

	var can  bool = false
	//log.Printf("[recover]lost idx %d\n", lrch.si.Lostindex)
	//defer log.Printf("[recover]recover idx end %d\n", lrch.si.Lostindex)
	var n uint16
start:
	lrch.shards = make([][]byte, 0)
	n++
	//log.Println("尝试第", n, "次")

	sl, _ := lrch.si.GetNeededShardList(lrch.si.Handle)

	//var number int
	var indexs []int16
	for i := sl.Front(); i != nil; i = i.Next() {
		indexs = append(indexs, i.Value.(int16))
	}

	//log.Println("[recover]need shard list", indexs, len(indexs))

	k := 0
	for _, idx := range indexs {
		k++
		peer := td.Locations[idx]
		if !activeNodeList.HasNodeid(peer.NodeId){
            lrch.si.ShardExist[idx] = 0
			continue
		}
		lrch.si.ShardExist[idx] = 1

		if can {
			continue
		}

        shard := re.tstdata[idx][:]
		//shard, err := lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number)
		//
		//// if there is some error, we should to try again
		//if err != nil {
		//	fmt.Println("[recover]first getshard error:",err)
		//	shard, err = lrch.le.GetShard(peer.NodeId, base58.Encode(td.Id), peer.Addrs, td.Hashs[idx], &number)
		//	if err != nil || len(shard) == 0 {
		//		fmt.Println("[recover]second getshard error:",err,"len shard=",len(shard))
		//		if k >= len(indexs) && n < 3{
		//			goto start
		//		}
		//		continue
		//	}
		//}
		//
		//if len(shard) == 0 {
		//	log.Println("[recover][ytlrc] shard is empty!!")
		//	if k >= len(indexs) && n < 3 {
		//		goto  start
		//	}
		//	continue
		//}

		status := lrch.si.AddShardData(lrch.si.Handle, shard)
		if status > 0{
			_, status2 := lrch.si.GetRebuildData(lrch.si)
			if status2 > 0 {        //rebuild success
				can = true
			}
		}else if status < 0 {     //rebuild failed
			if n < 3 {
				goto start
			}
		}else {
			if k >= len(indexs) && n < 3 {  //rebuild mode(hor, ver) over
				goto start
			}
		}
	}
	return can,nil
}

func (re *RecoverEngine)PreTstRecover(lrcshd lrcpkg.Shardsinfo, msg message.TaskDescription) (bool, error){
	hd, err := re.le.GetLRCHandler(&lrcshd)
	if err != nil {
		log.Printf("[recover]LRC 获取Handler失败%s", err)
		return false, err
	}
	can, err := re.RecoverTst(msg, hd)
    return can,err
}