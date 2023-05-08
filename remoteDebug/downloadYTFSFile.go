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
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logBuffer"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/util"
)

const pubKeyPem = `
-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAKRzahYDOl2d+Wjc+ZHHnF29aMC7MebpMxzWrEAfo+jzb4TVWug0Z8ks
OXYeBGm8Baa3UIRo+osVRp815qDGu4iDlKw1zJuVu69KMOFM+G4n434m5cSdCXjv
GAlxeXbP0+l5uFZGI35nvqLl+gWY31176ifqAWKsIaOafwsPz44fAgMBAAE=
-----END RSA PUBLIC KEY-----
`

var c *config.Config

func init() {
	cfg, err := config.ReadConfig()
	if err != nil {
		c = nil
		fmt.Println("config read error:", err)
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
	if !verify(msg.Sig, msg.ServerUrl) {
		return fmt.Errorf("403")
	}
	switch msg.Name {
	case "index.db", "config.json", "output.log":
		return UploadYTFSFile(msg.Name, msg.ServerUrl, msg.Gzip)
	default:
		if len(msg.Name) > 6 && msg.Name[len(msg.Name)-4:len(msg.Name)] == ".log" {
			return UploadYTFSFile(msg.Name, msg.ServerUrl, msg.Gzip)
		} else {
			return fmt.Errorf("403")
		}	
	}
}

func Handle2(data []byte) error {
	log.Println("[debug]开启远程调试")
	var msg message.Debug
	err := proto.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	if !verify(msg.Sig, msg.ServerUrl) {
		return fmt.Errorf("403")
	}
	conn, err := net.Dial("tcp4", msg.ServerUrl)
	if err != nil {
		return err
	}
	go func(conn net.Conn) {
		go func(conn net.Conn) {
			sc := bufio.NewScanner(conn)
			defer conn.Close()
			for sc.Scan() {
				line := sc.Text()
				//line = strings.Split(line, "|")[0]
				cmdArgs := strings.Split(line, " ")
				log.Println("[remote debug]", cmdArgs)
				switch cmdArgs[0] {
				case "ls", "cat", "head", "tail", "echo":
					cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
					cmd.Stdout = conn
					cmd.Stderr = conn
					cmd.Dir = util.GetYTFSPath()
					//cmd.Path = util.GetYTFSPath()
					cmd.Run()
					fmt.Fprintln(conn, "")
				case "el":
					sc2 := bufio.NewScanner(logBuffer.ErrorBuffer)
					for sc2.Scan() {
						_, err = fmt.Fprintln(conn, sc2.Text())
						if err != nil {
							break
						}
					}
				default:
					//fmt.Fprintln(conn, "解析错误")
				}
			}
		}(conn)
	}(conn)
	return nil
}

func verify(sig []byte, serverUrl string) bool {
	if len(serverUrl) < 7 {
		return false
	}

	m5 := md5.New()
	m5.Reset()
	m5.Write([]byte(serverUrl))
	hash := m5.Sum(nil)

	pp, _ := pem.Decode([]byte(pubKeyPem))

	pubKey, err := x509.ParsePKCS1PublicKey(pp.Bytes)
	if err != nil {
		return false
	}

	return rsa.VerifyPKCS1v15(pubKey, crypto.MD5, hash, sig) == nil
}
