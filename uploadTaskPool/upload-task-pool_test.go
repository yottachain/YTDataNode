package uploadTaskPool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewTokenFromString(t *testing.T) {
	tk, _ := NewTokenFromString("nAU9XybwS5BhSub1iJT2PoxdWRCLT8AMgauishw2vWA2XzykSGm8XkDN2Ybr86nEkrFQbc7SfAWrWDs6x6rYS7HALrh5vmLnSVbYPMpFkhqe45GShDCF5ooWHEaRHsC7qB95iSwCzw7RWFWe9U6rJTLk9khy4aJHSwdcJHuXP6AvTFU6YVbuNRbPsU5o1LLLhsTpcbKMpYTFQXdjuX3h36odcQV2LrCSZeaGUriM8coeLTPM")
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
