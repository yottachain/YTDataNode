package log_test

import (
	log "github.com/yottachain/YTDataNode/logger"
	"testing"
)

func TestLogger(t *testing.T) {
	log.SetFileLog()
	for i := 0; i < 200000; i++ {
		log.Println("写日志行", i)
	}
}
