package diskHash

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path"

	"github.com/mr-tron/base58"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/common"
)

func randShard(n int) map[common.IndexTableKey][]byte {
	var res = make(map[common.IndexTableKey][]byte, n)
	for i := 0; i < n; i++ {
		buf := make([]byte, config.GlobalShardSize*1024)
		rand.Read(buf)
		key := md5.Sum(buf)
		res[common.IndexTableKey{Hsh: key, Id: 0}] = buf
		log.Println("[diskHash] write_base58_key:", base58.Encode(key[:]))
	}
	return res
}
func RandWrite(ytfs *ytfs.YTFS, l uint) error {
	if l > 5 {
		return nil
	}
	_, err := ytfs.BatchPut(randShard(5 - int(l)))
	if err != nil {
		return err
	}
	return nil
}

func GetHash(ytfs *ytfs.YTFS) (string, error) {
	buf, err := ytfs.GetData(0)
	if err != nil || len(buf) != int(config.GlobalShardSize*1024) {
		err = fmt.Errorf("GetDiskHashError")
		return "diskHasherror", err
	}
	md5buf := md5.Sum(buf)
	log.Println("[diskHash] get_base58_key:", base58.Encode(md5buf[:]), "hex_string", hex.EncodeToString(md5buf[:]))
	return hex.EncodeToString(md5buf[:]), nil
}

func GetHead() []byte {
	cfg := config.DefaultConfig
	s1 := cfg.Storages[0]
	fl, err := os.OpenFile(s1.StorageName, os.O_RDONLY, 0644)
	if err != nil {
		return nil
	}
	defer fl.Close()

	buf := make([]byte, 64+config.GlobalShardSize*1024)
	fl.Read(buf)
	return buf
}

func CopyHead() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), "head.file"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err == nil {
		defer fl.Close()

		head := GetHead()
		fl.Write(head)
	} else {
		log.Println("[diskHash] copyHead openfile error:", err)
	}
}
