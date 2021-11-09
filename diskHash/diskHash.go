package diskHash

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/yottachain/YTDataNode/config"
)

func GetHash() (string, error) {
	cfg := config.DefaultConfig
	buf := bytes.NewBuffer([]byte{})
	ec := json.NewEncoder(buf)
	ec.Encode(cfg)

	md5buf := md5.Sum(buf.Bytes())
	return hex.EncodeToString(md5buf[:]), nil
}
