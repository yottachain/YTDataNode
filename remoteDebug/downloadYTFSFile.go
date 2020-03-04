package remoteDebug

import (
	"compress/gzip"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/util"
	"io"
	"net"
	"os"
	"path"
)

const block_size = 4096

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
	if compress {
		err := CompressYTFSFile(name)
		if err != nil {
			return err
		}
		fr, err := os.OpenFile(path.Join(util.GetYTFSPath(), name+".gz"), os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer fr.Close()
		conn, err := net.Dial("tcp4", addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		io.Copy(conn, fr)
	} else {
		fr, err := os.OpenFile(path.Join(util.GetYTFSPath(), name), os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer fr.Close()
		conn, err := net.Dial("tcp4", addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		io.Copy(conn, fr)
	}

	return nil
}

func Handle(data []byte) error {
	var msg message.DownloadYTFSFile
	err := proto.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	return UploadYTFSFile(msg.Name, msg.ServerUrl, msg.Gzip)
}
