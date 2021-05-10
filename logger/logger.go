package log

import (
	"fmt"
	"github.com/natefinch/lumberjack"
	"github.com/yottachain/YTDataNode/util"
	"io"
	"log"
	"net"
	"path"
	"time"
)

var FileLogger = newSyncWriter(&lumberjack.Logger{
	Filename:   path.Join(util.GetYTFSPath(), "output.log"),
	MaxSize:    128,
	Compress:   false,
	MaxAge:     7,
	MaxBackups: 1,
})

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func SetFileLog() {
	log.SetOutput(FileLogger)
	go LogService()
}

func LogService() {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9003")
	tcpService, _ := net.ListenTCP("tcp", addr)
	for {
		tcpConn, _ := tcpService.AcceptTCP()
		handLogConn(tcpConn)
	}
}

func handLogConn(conn *net.TCPConn) {
	log.SetOutput(io.MultiWriter(conn, FileLogger))
}

type syncWriter struct {
	dist io.Writer
	q    chan struct{}
}

func (s syncWriter) Write(p []byte) (n int, err error) {
	defer func() {
		select {
		case <-s.q:
		default:
		}
	}()

	select {
	case s.q <- struct{}{}:
		return s.dist.Write(p)
	case <-time.After(time.Second):
		return 0, fmt.Errorf("time out")
	}
}
func newSyncWriter(dist io.Writer) *syncWriter {
	return &syncWriter{
		dist,
		make(chan struct{}, 1),
	}
}

var Println = log.Println
var Printf = log.Printf
var Fatalln = log.Fatalln
var Fatalf = log.Fatalf
var Fatal = log.Fatal
