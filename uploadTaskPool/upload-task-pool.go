package uploadTaskPool

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/mr-tron/base58/base58"
	"github.com/satori/go.uuid"
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
	WaitToekns chan *Token
}

func New(size int) *UploadTaskPool {
	utp := UploadTaskPool{
		make([]*Token, size),
		size,
		sync.Mutex{},
		make(chan *Token, size),
	}
	return &utp
}

func (utp *UploadTaskPool) get() (*Token, error) {
	if index := utp.hasFreeToken(); index > -1 {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		token := &Token{index, id, time.Time{}}

		utp.Lock()
		defer utp.Unlock()

		utp.tokenMap[index] = token
		return token, nil
	} else {
		return nil, fmt.Errorf("upload queue bus")
	}
}

func (utp *UploadTaskPool) Get() (*Token, error) {
	tk, err := utp.get()
	if err != nil {
		return nil, err
	} else {
		tk.Tm = time.Now()
		return tk, nil
	}
}

func (utp *UploadTaskPool) FillQueue() {
	go func() {
		for {
			tk, err := utp.get()
			if err == nil {
				utp.WaitToekns <- tk
			}
			<-time.After(10 * time.Millisecond)
		}
	}()
}

func (utp *UploadTaskPool) GetTokenFromWaitQueue(ctx context.Context) (*Token, error) {
	select {
	case tk := <-utp.WaitToekns:
		tk.Tm = time.Now()
		return tk, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("upload queue bus")
	}
}

func (utp *UploadTaskPool) Check(tk *Token) bool {
	if tk == nil || utp.tokenMap[tk.Index] == nil {
		return false
	}
	return bytes.Equal(tk.UUID.Bytes(), utp.tokenMap[tk.Index].UUID.Bytes())
}

func (utp *UploadTaskPool) hasFreeToken() int {
	utp.Lock()
	defer utp.Unlock()
	var i = -1
	for index, tk := range utp.tokenMap {
		if utp.tokenMap[index] == nil {
			return index
			//	如果token时间不等于初始时间 且token > 10 秒 让出此位置
		} else if tk.Tm.Unix() != (time.Time{}).Unix() && time.Now().Sub(tk.Tm).Seconds() > 10 {
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

}
