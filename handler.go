package node

import (
	"fmt"

	"github.com/yottachain/YTDataNode/message"

	"github.com/golang/protobuf/proto"
	"github.com/yottachain/YTFS/common"
)

// WriteHandler 写入处理器
type WriteHandler struct {
	StorageNode
}

// GetHandler 获取回调处理函数
func (wh *WriteHandler) GetHandler(msgData []byte) []byte {
	var msg message.UploadShardRequest
	proto.Unmarshal(msgData, &msg)
	resCode := wh.getResponseCode(msg)
	res2client, err := msg.GetResponseToClientByCode(resCode)
	if err != nil {
		fmt.Println("Get res code fail:", err)
	}
	return res2client
}

func (wh *WriteHandler) getResponseCode(msg message.UploadShardRequest) int32 {
	// 1. 验证BP签名
	// if ok, err := msg.VerifyBPSIGN(
	// 	// 获取BP公钥
	// 	host.PubKey(wh.Host().Peerstore().PubKey(wh.GetBP(msg.BPDID))),
	// 	wh.Host().ID().Pretty(),
	// ); err != nil || ok == false {
	// 	fmt.Println(fmt.Errorf("Verify BPSIGN fail:%s", err))
	// 	return 100
	// }
	// 2. 验证数据Hash
	if msg.VerifyVHF(msg.DAT) == false {
		fmt.Println(fmt.Errorf("Verify VHF fail"))
		return 100
	}
	// 3. 将数据写入YTFS-disk
	var indexKey [32]byte
	copy(indexKey[:], msg.VHF[0:32])
	err := wh.YTFS().Put(common.IndexTableKey(indexKey), msg.DAT)
	if err != nil {
		fmt.Println(fmt.Errorf("Write data slice fail:%s", err))
		return 101
	}
	fmt.Println("return msg", 0)
	return 0
}

// DownloadHandler 下载处理器
type DownloadHandler struct {
	StorageNode
}

// GetHandler 获取处理器
func (dh *DownloadHandler) GetHandler(msgData []byte) []byte {
	var msg message.DownloadShardRequest
	var indexKey [32]byte
	copy(indexKey[:], msg.VHF[0:32])
	proto.Unmarshal(msgData, &msg)
	res := message.DownloadShardResponse{}
	resData, err := dh.YTFS().Get(common.IndexTableKey(indexKey))
	if err != nil {
		fmt.Println("Get data Slice fail:", err)
	}
	res.Data = resData
	resp, err := proto.Marshal(&res)
	if err != nil {
		fmt.Println("Marshar response data fail:", err)
	}
	fmt.Println("return msg", 0)
	return resp
}
