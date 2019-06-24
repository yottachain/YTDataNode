package node

import (
	"context"
	"fmt"
	"time"

	"github.com/mr-tron/base58/base58"

	"github.com/yottachain/YTDataNode/message"

	"github.com/golang/protobuf/proto"
	"github.com/yottachain/YTFS/common"
)

// WriteHandler 写入处理器
type WriteHandler struct {
	StorageNode
}

// Handle 获取回调处理函数
func (wh *WriteHandler) Handle(msgData []byte) []byte {
	var msg message.UploadShardRequest
	proto.Unmarshal(msgData, &msg)
	fmt.Println("超级节点签名:", msg.GetBPDSIGN())
	fmt.Println("用户签名:", msg.GetUSERSIGN())
	resCode := wh.saveSlice(msg)
	res2client, err := msg.GetResponseToClientByCode(resCode)
	bp := wh.Config().BPList[msg.BPDID]
	if err != nil {
		fmt.Println("Get res code 2 client fail:", err)
	}
	res2bp, err := msg.GetResponseToBPByCode(resCode, bp.ID, wh.Host().PrivKey())
	if err != nil {
		fmt.Println("Get res code fail:", err)
	}
	if err != nil {
		fmt.Println("Get res code 2 bp fail:", err)
	}
	if err = wh.Host().ConnectAddrStrings(bp.ID, bp.Addrs); err != nil {
		fmt.Println("Connect bp fail", err)
	}
	wh.Host().SendMsg(bp.ID, "/node/0.0.1", res2bp)
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

// Handle 获取处理器
func (dh *DownloadHandler) Handle(msgData []byte) []byte {
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

type RecoverHandler struct {
	StorageNode
}

func (rh *RecoverHandler) Handle(data []byte) []byte {
	var res []byte
	var msg message.TaskDescription
	proto.Unmarshal(data, &msg)

	return res
}

//type checkDataTask struct {
//	message.SpotCheckTask
//}

type CheckDataHandler struct {
	StorageNode
	TaskList []*message.SpotCheckTask
	TaskCh chan int
	snid int
}

func (ch *CheckDataHandler) Handle(data []byte, snid int) []byte {
	var msg message.SpotCheckTaskList
	proto.Unmarshal(data, &msg)
	ch.TaskList = msg.TaskList
	ch.snid = snid
	ch.Run()
	return message.MsgIDVoidResponse.Marshal([]byte{})
}



func (ch *CheckDataHandler)do(task *message.SpotCheckTask) error {
	var msg message.DownloadShardRequest
	var resmsg message.DownloadShardResponse
	ch.Host().ConnectAddrStrings(task.NodeId, []string{task.Addr})
	// 1. 获取分片
	stm,err := ch.Host().NewMsgStream(context.Background(),task.NodeId, "/node/0.0.1")
	if err != nil {
		return err

	}
	msg.VHF = task.VHF
	msgData,err := proto.Marshal(&msg)
	if err != nil {
		return err
	}
	resp,err:= stm.SendMsgGetResponse(message.MsgIDDownloadShardRequest.Marshal(msgData))
	if err != nil {
		return nil
	}
	err=message.MsgIDDownloadShardResponse.Unmarshal(resp, &resmsg)
	if err != nil {
		return err
	}
	// 2. 验证分片VHF
	if msg.VerifyVHF(resmsg.Data) {
		return nil
	} else {
		return fmt.Errorf("check unpass")
	}
	defer func (){
		<- ch.TaskCh
	}()
	return nil
}

func (ch *CheckDataHandler)Run(){
	ch.TaskCh = make(chan int, 5)
	var msg message.SpotCheckStatus
	sn := ch.Config().BPList[ch.snid]
	for k,v:=range ch.TaskList {
		ch.TaskCh <- 0
		err:=ch.do(v)
		// 如果获取分片的过程中报错，或者数据校验报错都会校验失败
		if err != nil {
			msg.InvalidNodeList = append(msg.InvalidNodeList, v.Id)
		}
		msg.Percent = int32(k%len(ch.TaskList))
		if k % 10 == 0 {
			ctx ,_:= context.WithTimeout(context.Background(),30 * time.Second)
			ch.Host().ConnectAddrStrings(sn.ID, sn.Addrs)
			stm,err := ch.Host().NewMsgStream(ctx, sn.ID, "/node/0.0.1")
			fmt.Printf("create conn fail %s\n", err)
			msgData,err:=proto.Marshal(&msg)
			stm.SendMsg(message.MsgIDCheckStatus.Marshal(msgData))
		}
	}
}
