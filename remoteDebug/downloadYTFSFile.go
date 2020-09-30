package remoteDebug

import (
	"bufio"
	"compress/gzip"
	"crypto"
	"crypto/md5"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/util"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
)

const pubKeyPem = `
-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAZEz5xZs8ip2cGy8L06mMwxLZNFD7RRqpp3pJy3zCWui6bO2O667+Fwn
j2VxGbwgUALuKgbY+woxJ3jaSURC+g8IyW8LqUPnz8fqnivGjlIRqN/JLEKgKr8+
YhWPZHSVvcAh1ZKY3f3U0iTN2vMHdFwgC/zbp6FLxb3CNJolc5J5AgMBAAE=
-----END RSA PUBLIC KEY-----
`

var c *config.Config

func init() {
	cfg, err := config.ReadConfig()
	if err != nil {
		c = nil
		fmt.Println("config read error")
	}

	c = cfg
}

func Compress(name string) error {
	fi, err := os.Stat(name)
	if err != nil {
		return err
	}

	fr, err := os.OpenFile(name, os.O_RDONLY, fi.Mode())
	if err != nil {
		return err
	}
	defer fr.Close()

	fw, err := os.OpenFile(fmt.Sprintf("%s.gz", name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fi.Mode())
	if err != nil {
		return err
	}
	defer fw.Close()

	gw := gzip.NewWriter(fw)
	defer gw.Close()

	gw.Header.Name = fi.Name()

	if err != nil {
		return err
	}

	io.Copy(gw, fr)

	return nil
}

func CompressYTFSFile(name string) error {
	filename := path.Join(util.GetYTFSPath(), name)
	return Compress(filename)
}

func UploadYTFSFile(name string, addr string, compress bool) error {
	fn := fmt.Sprintf("upload/%d-%s", c.IndexID, name)
	if compress {
		fn = fn + ".gz"
		err := CompressYTFSFile(name)
		if err != nil {
			return err
		}
		fr, err := os.OpenFile(path.Join(util.GetYTFSPath(), name+".gz"), os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer fr.Close()

		_, err = http.Post("http://"+path.Join(addr, fn), "application/octet-stream", fr)
		if err != nil {
			log.Println(err)
		}
		log.Println("[debug]", "下载成功", path.Join(addr, fn))
	} else {
		fr, err := os.OpenFile(path.Join(util.GetYTFSPath(), name), os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer fr.Close()

		_, err = http.Post("http://"+path.Join(addr, fn), "application/octet-stream", fr)
		if err != nil {
			log.Println(err)
		}
		log.Println("[debug]", "下载成功", path.Join(addr, fn))
	}

	return nil
}

func Handle(data []byte) error {
	log.Println("[debug]下载请求")
	var msg message.DownloadYTFSFile
	err := proto.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	if !verify(msg.Sig) {
		return fmt.Errorf("403")
	}
	switch msg.Name {
	case "index.db", "config.json", "output.log":
		return UploadYTFSFile(msg.Name, msg.ServerUrl, msg.Gzip)
	default:
		return fmt.Errorf("403")
	}
}

func Handle2(data []byte) error {
	log.Println("[debug]开启远程调试")
	var msg message.Debug
	err := proto.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	if !verify(msg.Sig) {
		return fmt.Errorf("403")
	}
	conn, err := net.Dial("tcp4", msg.ServerUrl)
	if err != nil {
		return err
	}
	go func(conn net.Conn) {
		go func(conn net.Conn) {
			sc := bufio.NewScanner(conn)
			for sc.Scan() {
				line := sc.Text()
				cmdArgs := strings.Split(line, " ")
				log.Println("[remote debug]", cmdArgs)
				switch cmdArgs[0] {
				case "ls", "cat", "head", "tail":
					cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
					cmd.Stdout = conn
					cmd.Stderr = conn
					cmd.Path = util.GetYTFSPath()
					cmd.Start()
				default:
				}
			}
		}(conn)
	}(conn)
	return nil
}

func verify(sig []byte) bool {
	m5 := md5.New()
	m5.Reset()
	m5.Write([]byte("yotta debug"))
	hash := m5.Sum(nil)

	pp, _ := pem.Decode([]byte(pubKeyPem))

	pubKey, err := x509.ParsePKCS1PublicKey(pp.Bytes)
	if err != nil {
		return false
	}

	return rsa.VerifyPKCS1v15(pubKey, crypto.MD5, hash, sig) == nil
}
