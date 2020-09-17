package TaskPool

import (
	"bytes"
	"encoding/gob"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58/base58"
	"github.com/satori/go.uuid"
	"time"
)

type Token struct {
	UUID uuid.UUID
	PID  peer.ID
	Tm   time.Time
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

func NewToken() *Token {
	tk := new(Token)
	id := uuid.NewV4()

	tk.UUID = id
	tk.Tm = time.Time{}
	return tk
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

func (tk *Token) IsOuttime(ttl time.Duration) bool {
	if tk.Tm.Unix() != (time.Time{}).Unix() && time.Now().Sub(tk.Tm) > ttl {
		return true
	}
	return false
}

func (tk *Token) Reset() {
	if tk != nil {
		tk.Tm = time.Now()
	}
}
