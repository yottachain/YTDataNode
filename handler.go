package node

import (
	"fmt"

	"github.com/mr-tron/base58/base58"

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
	fmt.Println("超级节点签名:", msg.GetBPDSIGN())
	fmt.Println("用户签名:", msg.GetUSERSIGN())
	resCode := wh.saveSlice(msg)
	res2client, err := msg.GetResponseToClientByCode(resCode)
	if err != nil {
		fmt.Println("Get res code 2 client fail:", err)
	}
	res2bp, err := msg.GetResponseToBPByCode(resCode, "16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", wh.Host().PrivKey())
	if err != nil {
		fmt.Println("Get res code fail:", err)
	}
	if err != nil {
		fmt.Println("Get res code 2 bp fail:", err)
	}
	if err = wh.Host().ConnectAddrStrings("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", []string{
		"/ip4/172.21.0.13/tcp/9999",
		"/ip4/152.136.11.202/tcp/9999",
	}); err != nil {
		fmt.Println("Connect bp fail", err)
	}
	wh.Host().SendMsg("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", "/node/0.0.1", res2bp)
	fmt.Println("return client")
	defer func() {
		err := recover()
		if err != nil {
			fmt.Println("report to bp error", err)
		}
	}()
	return res2client
}

func (wh *WriteHandler) saveSlice(msg message.UploadShardRequest) int32 {
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
		if err.Error() == "YTFS: hash key conflict happens" {
			return 102
		}
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
	proto.Unmarshal(msgData, &msg)
	fmt.Println("get vhf:", base58.Encode(msg.VHF))

	for k, v := range msg.VHF {
		if k >= 32 {
			break
		}
		indexKey[k] = v
	}
	res := message.DownloadShardResponse{}
	resData, err := dh.YTFS().Get(common.IndexTableKey(indexKey))
	if msg.VerifyVHF(resData) {
		fmt.Println("data verify success")
	}
	if err != nil {
		fmt.Println("Get data Slice fail:", err)
	}
	res.Data = resData
	resp, err := proto.Marshal(&res)
	if err != nil {
		fmt.Println("Marshar response data fail:", err)
	}
	fmt.Println("return msg", 0)
	return append(message.MsgIDDownloadShardResponse.Bytes(), resp...)
}
