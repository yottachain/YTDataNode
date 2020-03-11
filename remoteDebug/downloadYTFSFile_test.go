package remoteDebug

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/message"
	"io"
	"os"
	"os/exec"
	"path"
	"testing"
)

func MakeFile(fileDir string) error {
	filename := path.Join(fileDir, "test.data")
	cmd := exec.Command("dd", "if=/dev/urandom", "bs=1m", "count=1", "of="+filename)
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

func ClearFile(fileDir string) error {
	filename := path.Join(fileDir, "test.data")
	return os.Remove(filename)
}

func SumMd5(fileDir string) (string, error) {
	filename := path.Join(fileDir, "test.data")

	md := md5.New()
	md.Reset()
	fr, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer fr.Close()
	io.Copy(md, fr)
	res := md.Sum(nil)
	return hex.EncodeToString(res), nil
}

func TestCompress(t *testing.T) {
	dir := "/Users/mac/go/src/github.com/yottachain/YTDataNode/downloadIndexDb"
	err := MakeFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	md5str, err := SumMd5(dir)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(md5str)

	if err := Compress(path.Join(dir, "test.data")); err != nil {
		t.Fatal(err)
	}
	if err := ClearFile(dir); err != nil {
		t.Fatal(err)
	}
	//if err := UnCompress(path.Join(dir, "test.data")); err != nil {
	//	t.Fatal(err)
	//}
	//md5str2, err := SumMd5(dir)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//if md5str2 == md5str {
	//	t.Log("success")
	//} else {
	//	fmt.Println(md5str, md5str2)
	//	t.Fatal("fail")
	//
	//}
}

func TestHandle(t *testing.T) {
	var msg message.DownloadYTFSFile
	msg.Name = "config.json"
	msg.ServerUrl = "112.126.83.77:10000"
	msg.Gzip = true
	buf, err := proto.Marshal(&msg)

	if err != nil {
		t.Fatal(err)
	}

	err = Handle(buf)
	if err != nil {
		t.Fatal(err)
	}
}
