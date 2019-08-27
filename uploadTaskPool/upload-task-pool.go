package uploadTaskPool

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/mr-tron/base58/base58"
	"github.com/satori/go.uuid"
	log "github.com/yottachain/YTDataNode/logger"
	"sync"
	"time"
)

type Token struct {
	Index int
	UUID  uuid.UUID
	Tm    time.Time
}

func (tk *Token) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	ed := gob.NewEncoder(buf)
	if tk == nil {
		return []byte{}
	}
	ed.Encode(tk)

	return buf.Bytes()
}

func (tk *Token) FillFromBytes(tkdata []byte) error {
	buf := bytes.NewReader(tkdata)
	dd := gob.NewDecoder(buf)
	return dd.Decode(tk)
}

func (tk *Token) FillFromString(tkstring string) error {
	data, err := base58.Decode(tkstring)
	if err != nil {
		return err
	}
	buf := bytes.NewReader(data)
	dd := gob.NewDecoder(buf)
	return dd.Decode(tk)
}

func NewTokenFromBytes(tkdata []byte) (*Token, error) {
	tk := new(Token)
	err := tk.FillFromBytes(tkdata)
	if err != nil {
		return nil, err
	} else {
		return tk, nil
	}
}

func NewTokenFromString(tkstring string) (*Token, error) {
	tk := new(Token)
	err := tk.FillFromString(tkstring)
	if err != nil {
		return nil, err
	} else {
		return tk, nil
	}
}

func (tk *Token) String() string {
	return base58.Encode(tk.Bytes())
}

type UploadTaskPool struct {
	tokenMap []*Token
	size     int
	sync.Mutex
}

func New(size int) *UploadTaskPool {
	utp := UploadTaskPool{
		make([]*Token, size),
		size,
		sync.Mutex{},
	}
	return &utp
}

func (utp *UploadTaskPool) Get() (*Token, error) {
	utp.Lock()
	defer utp.Unlock()
	if index := utp.hasFreeToken(); index > -1 {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		token := &Token{index, id, time.Now()}
		utp.tokenMap[index] = token

		return token, nil
	} else {
		log.Printf("[task pool] get token error: free task is 0/n")
		return nil, fmt.Errorf("upload queue bus")
	}
}

func (utp *UploadTaskPool) Check(tk *Token) bool {
	return bytes.Equal(tk.UUID.Bytes(), utp.tokenMap[tk.Index].UUID.Bytes())
}

func (utp *UploadTaskPool) hasFreeToken() int {
	var i = -1
	for index, tk := range utp.tokenMap {
		if utp.tokenMap[index] == nil {
			return index
		} else if time.Now().Sub(tk.Tm).Seconds() > 10 {
			return index
		}
	}
	return i
}

func (utp *UploadTaskPool) Put(index int) {
	utp.Lock()
	defer utp.Unlock()
	if index > utp.size {
		return
	}
	utp.tokenMap[index] = nil
	//log.Printf("[task pool]put task success %d/%d\n", index, utp.size)
}

//func (utp *UploadTaskPool) Len() int {
//	return len(utp.free)
//}
