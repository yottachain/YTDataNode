package message

import (
	"bytes"
	"crypto"
	"encoding/binary"
	"fmt"
	host "yottachain/ytfs-p2p-host"

	"github.com/golang/protobuf/proto"

	"github.com/libp2p/go-libp2p-peer"
)

// VerifyVHF 验证 DAT sha3 256 和vhf 是否相等
func (req *UploadShardRequest) VerifyVHF(data []byte) bool {
	sha3 := crypto.SHA3_256.New()
	return bytes.Equal(sha3.Sum(data), req.VHF[:])
}

// VerifyBPSIGN 验证上传请求BP签名
func (req *UploadShardRequest) VerifyBPSIGN(pubkey host.PubKey, nodeid string) (bool, error) {
	buf := bytes.NewBuffer([]byte{})
	buf.Reset()
	binary.Write(buf, binary.BigEndian, nodeid)
	binary.Write(buf, binary.BigEndian, req.VBI)
	return pubkey.Verify(buf.Bytes(), req.GetBPDSIGN())
}

// GetResponseToBPByCode 生成返回消息
func (req *UploadShardRequest) GetResponseToBPByCode(code int32, nodeID peer.ID, privkey host.PrivKey) ([]byte, error) {
	var res UploadShardResponse
	res.RES = code
	res.VHF = req.VHF
	res.VBI = req.VBI
	bts, err := privkey.Sign([]byte(fmt.Sprintf("%s%d", nodeID.Pretty(), req.VBI)))
	if err != nil {
		return nil, fmt.Errorf("Make response data fail:%s", err)
	}
	res.USERSIGN = bts
	return proto.Marshal(&res)
}

// GetResponseToClientByCode 生成客户端返回消息
func (req *UploadShardRequest) GetResponseToClientByCode(code int32) ([]byte, error) {
	var res UploadShard2CResponse
	res.RES = code
	return proto.Marshal(&res)
}
