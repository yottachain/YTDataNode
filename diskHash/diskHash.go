package diskHash

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	ytfs "github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/common"
	"os"
)

const CheckBlockSize = 64 + 16*1024

func randShard(n int) map[common.IndexTableKey][]byte {
	var res = make(map[common.IndexTableKey][]byte, n)
	for i := 0; i < 5; i++ {
		buf := make([]byte, 16)
		rand.Read(buf)
		key := md5.Sum(buf)
		res[key] = buf
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
