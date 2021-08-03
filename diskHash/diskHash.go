package diskHash

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/mr-tron/base58"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	ytfs "github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/common"
	"os"
	"path"
)

const CheckBlockSize = 64 + 16*1024

func randShard(n int) map[common.IndexTableKey][]byte {
	var res = make(map[common.IndexTableKey][]byte, n)
	for i := 0; i < 5; i++ {
		buf := make([]byte, 16*1024)
		rand.Read(buf)
		key := md5.Sum(buf)
		res[key] = buf
		log.Println("[diskHash] base58_key:",base58.Encode(key[:]))
	}
	return res
}
func randWrite(ytfs *ytfs.YTFS) error {
	_, err := ytfs.BatchPut(randShard(5))
	if err != nil {
		return err
	}
	return nil
}

func GetHash(ytfs *ytfs.YTFS) (string, error) {
	cfg := config.DefaultConfig
	l := ytfs.Len()
	if l < 5 {
		log.Println("[diskHash] ytfs_len:",l)
		err := randWrite(ytfs)
		if err != nil {
			return "", fmt.Errorf("write ytfs checkData error")
		}
	}

	s1 := cfg.Storages[0]
	fl, err := os.OpenFile(s1.StorageName, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer fl.Close()

	buf := make([]byte, CheckBlockSize)
	n, err := fl.Read(buf)
	if err != nil {
		return "", err
	}
	if n < CheckBlockSize {
		return "", fmt.Errorf("n < checkBlockSize %d\\%d", n, CheckBlockSize)
	}

	md5buf := md5.Sum(buf)
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

	buf := make([]byte, CheckBlockSize)
	fl.Read(buf)
	return buf
}

func CopyHead() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), "head.file"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err == nil {
		defer fl.Close()

		head := GetHead()
		fl.Write(head)
	}

}
