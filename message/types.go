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
	MsgIDSliceCompareReq       msgType = 0x1818
	MsgIDSliceCompareResp      msgType = 0x1819
	MsgIDSliceCompareStatusReq msgType = 0x1820
	MsgIDSliceCompareStatusResp msgType = 0x1821
	MsgIDCpDelStatusfileReq    msgType =0x1822
	MsgIDCpDelStatusfileResp   msgType =0x1823
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
	MsgIDTaskDescriptionCP     msgType = 0xc258
	MsgIDLRCTaskDescription    msgType = 0x68b3
	MsgIDMultiTaskOPResult     msgType = 0x1b31
	MsgIDMultiTaskOPResultRes  msgType = 0x93e4
	MsgIDListDNIReq            msgType = 0x4bc6
	MsgIDListDNIResp           msgType = 0xd6cb
	MsgIDDownloadYTFSFile      msgType = 0x1b32
	MsgIDDebug                 msgType = 0x1b33
	MsgIDSleepReturn           msgType = 0xe75c
	MsgIDSelfVerifyReq         msgType = 0xd97a
	MsgIDSelfVerifyResp        msgType = 0x58b7
	MsgIDSelfVerifyQueryReq    msgType = 0xd97b
	MsgIDSelfVerifyQueryResp   msgType = 0x58b8
	MsgIDDownloadTKCheck       msgType = 0x1b34
	MsgIDTestMinerPerfTask     msgType = 0xe76a
	MsgIDTestGetBlock          msgType = 0xe76b
	MsgIDTestGetBlockRes       msgType = 0xe76c
	MsgIDError                 msgType = 0x5913
	MsgIDGcReq                 msgType = 0xe87a
	MsgIDGcResp                msgType = 0xe87b
	MsgIDGcStatusReq           msgType = 0xe87e
	MsgIDGcStatusResp          msgType = 0xe87f
	MsgIDGcdelStatusfileReq    msgType = 0xe88e
	MsgIDGcdelStatusfileResp   msgType = 0xe88f

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
