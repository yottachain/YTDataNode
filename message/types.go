package message

import (
	"bytes"
	"encoding/binary"
)

const (

	// MsgIDUploadShardRequest 上传分片消息
	MsgIDUploadShardRequest msgType = 0xcb05
	// MsgIDUploadShardResponse 上传分片消息返回
	MsgIDUploadShardResponse msgType = 0x870b
	// MsgIDVoidResponse 空返回
	MsgIDVoidResponse          msgType = 0xe64f
	MsgIDUploadShard2CResponse msgType = 0x1978
	MsgIDDownloadShardRequest  msgType = 0x1757
	MsgIDDownloadShardResponse msgType = 0x7a56
	MsgIDNodeRegReq            msgType = 0x12aa
	MsgIDNodeRegResp           msgType = 0xfb92
	MsgIDStatusRepReq          msgType = 0xc9a9
	MsgIDStatusRepResp         msgType = 0xfa09
	MsgIDTaskDescript          msgType = 0xd761
	MsgIDTaskOPResult          msgType = 0x16f3
	MsgIDSpotCheckTaskList     msgType = 0x903a
	MsgIDSpotCheckStatus       msgType = 0xa583
	MsgIDString                msgType = 0x0000
)

type msgType int32

func (mt msgType) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int16(mt))
	return buf.Bytes()
}
func (mt msgType) Value() int32 {
	return int32(mt)
}
