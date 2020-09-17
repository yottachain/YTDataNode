package TaskPool

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	"testing"
)

func TestUploadTaskPool_Check(t *testing.T) {
	go Utp().FillToken()
	tk, err := Utp().Get(context.Background(), peer.ID("111"), 0)
	if err != nil {
		t.Fatalf(err.Error())
	} else {
		t.Log(tk.String())
	}
}
