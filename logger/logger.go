package log

import (
	"github.com/yottachain/YTDataNode/util"
	"io"
	"log"
	"os"
)

func init() {
	file := util.GetLogFile("output.log")
	log.SetOutput(io.MultiWriter(file, os.Stdout))
}

var Println = log.Println
var Printf = log.Printf
var Fatalln = log.Fatalln
var Fatalf = log.Fatalf
var Fatal = log.Fatal
