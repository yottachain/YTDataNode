package install

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
)

func checkRockDB() bool {
	ok, err := PathExists("/usr/local/rocksdb")
	if err != nil {
		return false
	}
	return ok
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func installRocksDB() error {
	switch runtime.GOOS {
	case "linux":
		return installRocksDBLinux()
	default:
		return nil
	}
}

func installRocksDBLinux() error {
	resp, err := http.Get("https://yottachain.oss-cn-beijing.aliyuncs.com/yottachain/rocksdb_x86_64.tar.gz")
	if err != nil {
		return err
	} else {
		fl, err := os.OpenFile("rocksdb_x86_64.tar.gz", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		io.Copy(fl, resp.Body)
		fl.Close()
		resp.Body.Close()
	}

	shellBuf := bytes.NewBuffer([]byte{})
	fmt.Fprintln(shellBuf, "tar xzvf rocksdb_x86_64.tar.gz")
	fmt.Fprintln(shellBuf, "cp -rf rocksdb /usr/local/")
	fmt.Fprintln(shellBuf, "echo /usr/local/rocksdb/lib > /etc/ld.so.conf.d/rocksdb.conf")
	fmt.Fprintln(shellBuf, "echo /usr/local/rocksdb/lib64 >> /etc/ld.so.conf.d/rocksdb.conf")
	fmt.Fprintln(shellBuf, "ldconfig")

	cmd := exec.Command("bash", "-c", shellBuf.String())
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

func InstallLib() {
	if !checkRockDB() {
		fmt.Println("开始安装rocksdb")
		if err := installRocksDB(); err != nil {
			fmt.Println("安装失败", err)
		} else {
			fmt.Println("安装完成")
		}
	}
}

func init() {
	InstallLib()
}
