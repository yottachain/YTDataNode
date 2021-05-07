package log

import (
	"github.com/natefinch/lumberjack"
	"github.com/yottachain/YTDataNode/util"
	"io"
	"log"
	"net"
	"path"
)

var FileLogger = &lumberjack.Logger{
	Filename:   path.Join(util.GetYTFSPath(), "output.log"),
	MaxSize:    128,
	Compress:   false,
	MaxAge:     7,
	MaxBackups: 5,
}

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

var Println = func(args ...interface{}) {
	go log.Println(args...)
}
var Printf = func(fmtStr string, args ...interface{}) {
	go log.Printf(fmtStr, args...)
}
var Fatalln = func(args ...interface{}) {
	go log.Fatalln(args...)
}
var Fatalf = func(fmtStr string, args ...interface{}) {
	go log.Fatalf(fmtStr, args...)
}
var Fatal = log.Fatal
