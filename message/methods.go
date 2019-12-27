package message

import (
	"bytes"
	"crypto"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	lc "github.com/libp2p/go-libp2p-core/crypto"
	ci "github.com/yottachain/YTCrypto"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
)

// VerifyVHF 验证 DAT sha3 256 和vhf 是否相等
func (req *UploadShardRequest) VerifyVHF(data []byte) bool {
	sha := crypto.MD5.New()
	sha.Write(data)
	return bytes.Equal(sha.Sum(nil), req.VHF[:])
}

// VerifyVHF 验证 DAT sha3 256 和vhf 是否相等
func (req *DownloadShardRequest) VerifyVHF(data []byte) bool {
	sha := crypto.MD5.New()
	sha.Write(data)
	return bytes.Equal(sha.Sum(nil), req.VHF[:])
}

// VerifyBPSIGN 验证上传请求BP签名
func (req *UploadShardRequest) VerifyBPSIGN(pubkey lc.PubKey, nodeid string) (bool, error) {
	buf := bytes.NewBuffer([]byte{})
	buf.Reset()
	binary.Write(buf, binary.BigEndian, nodeid)
	binary.Write(buf, binary.BigEndian, req.VBI)
	return pubkey.Verify(buf.Bytes(), req.GetBPDSIGN())
}

// GetResponseToBPByCode 生成返回消息
func (req *UploadShardRequest) GetResponseToBPByCode(code int32, nodeID string, privkey lc.PrivKey) ([]byte, error) {
	var res UploadShardResponse
	res.RES = code
	res.VHF = req.VHF
	res.VBI = req.VBI
	res.SHARDID = req.SHARDID
	bts, err := privkey.Sign([]byte(fmt.Sprintf("%s%d", nodeID, req.VBI)))
	if err != nil {
		return nil, fmt.Errorf("Make response data fail:%s", err)
	}
	res.USERSIGN = bts
	resdata, err := proto.Marshal(&res)
	if err != nil {
		return nil, err
	}
	return append(MsgIDUploadShardResponse.Bytes(), resdata...), nil
}

// GetResponseToClientByCode 生成客户端返回消息
func (req *UploadShardRequest) GetResponseToClientByCode(code int32, privkey string) ([]byte, error) {
	var res UploadShard2CResponse
	if code == 0 || code == 102 {
		pk, err := util.Libp2pPkey2eosPkey(privkey)
		if err != nil {
			return nil, err
		}
		dnsig, err := ci.Sign(pk, req.VHF)
		if err == nil {
			res.DNSIGN = dnsig
		} else {
			log.Printf("[dn sign]sign fail %s\n", err)
		}

	}
	res.RES = code
	resData, err := proto.Marshal(&res)
	if err != nil {
		return nil, err
	}
	log.Printf("[dn sign]%s-%s\n", res.DNSIGN, privkey)
	return append(MsgIDUploadShard2CResponse.Bytes(), resData...), nil
}
