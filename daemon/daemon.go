package daemon

import (
	//"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/exec"
)

const (
	name = "ytfsd"
	port = 9000
)

var DaemonCMD = &cobra.Command{
	Short: "yottachain启动矿机守护进程",
	Use:   "daemon",
	Run: func(cmd *cobra.Command, args []string) {
		c := exec.Command(os.Args[1], os.Args[1:]...)
		c.Start()
	},
}
