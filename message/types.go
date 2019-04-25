package message

import (
	"bytes"
	"encoding/binary"
)

const (
	// MsgIDUploadShardRequest 上传分片消息
	MsgIDUploadShardRequest = 0xcb05
	// MsgIDUploadShardResponse 上传分片消息返回
	MsgIDUploadShardResponse = 0x870b
	// MsgIDVoidResponse 空返回
	MsgIDVoidResponse          = 0xe64f
	MsgIDUploadShard2CResponse = 0x1978
	MsgIDDownloadShardRequest  = 0x1757
	MsgIDDownloadShardResponse = 0x7a56
)

type msgTypeInt int16

func (mt msgTypeInt) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, mt)
	return buf.Bytes()
}
