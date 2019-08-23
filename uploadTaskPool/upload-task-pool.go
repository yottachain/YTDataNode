package uploadTaskPool

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/mr-tron/base58/base58"
	"github.com/satori/go.uuid"
	"sync"
)

type Token struct {
	index int
	uuid.UUID
}

func (tk *Token) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	ed := gob.NewEncoder(buf)
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
	tokenMap []*uuid.UUID
	size     int
	free     chan int
	sync.Mutex
}

func New(size int) *UploadTaskPool {
	utp := UploadTaskPool{
		make([]*uuid.UUID, size),
		size,
		make(chan int, size),
		sync.Mutex{},
	}
	for i := 0; i < utp.size; i++ {
		utp.Put(i)
	}
	return &utp
}

func (utp *UploadTaskPool) Get() (*Token, error) {
	utp.Lock()
	defer utp.Unlock()

	if utp.hasFreeToken() {
		token, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		index := <-utp.free
		utp.tokenMap[index] = &token
		return &Token{index, token}, nil
	} else {
		return nil, fmt.Errorf("upload queue bus")
	}
}

func (utp *UploadTaskPool) Check(tk *Token) bool {
	return bytes.Equal(tk.UUID.Bytes(), utp.tokenMap[tk.index].Bytes())
}

func (utp *UploadTaskPool) hasFreeToken() bool {
	return len(utp.free) > 0
}

func (utp *UploadTaskPool) Put(index int) {
	if index > utp.size {
		return
	}
	utp.Lock()
	defer utp.Unlock()
	utp.tokenMap[index] = nil
	utp.free <- index
}
