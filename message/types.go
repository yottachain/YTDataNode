package message

import (
	"bytes"
	"encoding/binary"
)

const (
	MsgIDNodeCapacityRequest   msgType = 0xc487
	MsgIDNodeCapacityResponse  msgType = 0xe684
	MsgIDUploadShardRequest    msgType = 0xCB05
	MsgIDUploadShardResponse   msgType = 0x870b
	MsgIDVoidResponse          msgType = 0xe64f
	MsgIDUploadShard2CResponse msgType = 0x1978
	MsgIDDownloadShardRequest  msgType = 0x1757
	MsgIDDownloadShardResponse msgType = 0x7a56
	MsgIDNodeRegReq            msgType = 0x12aa
	MsgIDNodeRegResp           msgType = 0xfb92
	MsgIDStatusRepReq          msgType = 0xc9a9
	MsgIDStatusRepResp         msgType = 0xfa09
	MsgIDTaskDescript          msgType = 0xd761
	MsgIDTaskDescriptCP        msgType = 0xc258
	MsgIDTaskOPResult          msgType = 0x16f3
	MsgIDSpotCheckTaskList     msgType = 0x903a
	MsgIDSpotCheckStatus       msgType = 0xa583
	MsgIDString                msgType = 0x0011
	MsgIDMultiTaskDescription  msgType = 0x2cb0
	MsgIDLRCTaskDescription    msgType = 0x68b3
	MsgIDMultiTaskOPResult     msgType = 0x1b31
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
