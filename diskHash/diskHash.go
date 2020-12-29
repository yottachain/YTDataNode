package diskHash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/yottachain/YTDataNode/config"
	ytfs "github.com/yottachain/YTFS"
	"os"
)

const CheckBlockSize = 64 + 16*1024

func GetHash(ytfs *ytfs.YTFS) (string, error) {
	cfg := config.DefaultConfig
	l := ytfs.Len()
	if l < 5 {
		return "", fmt.Errorf("ytfs len < 5")
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
