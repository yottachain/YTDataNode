package uploadTaskPool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewTokenFromString(t *testing.T) {
	tk, _ := NewTokenFromString("nATevdrWzjgGogypcG91S4QLdYxZAnE2bWWcf93WZd2Gt9YXN8LrS8Zrj3G9WLKAPVovfwLzBE7D84kxD73M49G7YNwcGkmqPiMbxzAtnM5BXw9Ld1DHvopiH2nGWhXDXm62ZeHiSPXAzKNRdisW4izvM3knfJu1iphURHp4KCdzWJSdRxwPZnoiczoZytmeu6NRun1NXmHZywYUcUEPCX5pUrz3CoWLFj7JYW8YGQGadoKd")
	t.Log(tk.Tm)
}

func TestUploadTaskPool_Check(t *testing.T) {
	tp := New(10, time.Second*5, time.Millisecond*100)
	go tp.FillToken(context.Background())

	//go func() {
	//	for {
	//		tk, err := tp.Get(context.Background(), peer.ID("11"))
	//		fmt.Printf("tk: %s ,err: %s \n", tk.String(), err)
	//	}
	//}()
	select {}
}

func TestTime(t *testing.T) {
	for {
		fmt.Println(time.Time{}.Unix())
		<-time.After(time.Second * 3)
	}
}
