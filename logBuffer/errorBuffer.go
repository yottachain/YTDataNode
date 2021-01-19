package logBuffer

import (
	"bytes"
	"log"
)

var ErrorBuffer = bytes.NewBuffer(make([]byte, 1024))
var ErrorLogger = log.New(ErrorBuffer, "", 0644)

func init() {
	ErrorLogger.SetFlags(log.LstdFlags | log.Lshortfile)
}
