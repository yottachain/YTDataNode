package daemon

import (
	//"fmt"
	"github.com/spf13/cobra"
	"github.com/takama/daemon"
	"net"
	"os"
	"os/exec"
)

const (
	name = "ytfsd"
	port = 9000
)

type Service struct {
	daemon.Daemon
}

var service Service
var listener net.Listener
var listen = make(chan net.Conn, 100)

var DaemonCMD = &cobra.Command{
	Short: "yottachain启动矿机守护进程",
	Use:   "daemon",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func installService() {
	var cmd exec.Cmd
	cmd.Env = os.Environ()
}
