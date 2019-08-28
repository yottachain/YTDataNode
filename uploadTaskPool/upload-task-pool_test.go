package uploadTaskPool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestGetSuccess(t *testing.T) {
	utp := New(1000)
	for i := 0; i < 1000; i++ {
		tk, err := utp.Get()
		if err != nil {
			t.Error(err)
		} else {
			t.Log(tk.String())
		}
	}
}

func TestGetFail(t *testing.T) {
	utp := New(11)
	for i := 0; i < 12; i++ {
		tk, err := utp.Get()
		if err != nil {
			t.Error(err)
		} else {
			t.Log(tk.String())
		}
	}
}

func TestGetSuccessAndPut(t *testing.T) {
	utp := New(1000)
	for i := 0; i < 1000; i++ {
		tk, err := utp.Get()
		if err != nil {
			t.Error(err)
		} else {
			t.Log(tk.String())
		}
	}
	for i := 0; i < 1000; i++ {
		utp.Put(i)
	}
	for i := 0; i < 1000; i++ {
		tk, err := utp.Get()
		if err != nil {
			t.Error(err)
		} else {
			t.Log(tk.String())
		}
	}
}

func TestCheckToken(t *testing.T) {
	utp := New(10)
	for i := 0; i < 10; i++ {
		tk, err := utp.Get()
		if err != nil {
			t.Error(err)
		} else {
			ntk := Token{}
			err = ntk.FillFromBytes(tk.Bytes())
			if err != nil {
				t.Error(err)
			}
			t.Log(tk.UUID.String())
			t.Log(ntk.UUID.String())
			if utp.Check(&ntk) {
				t.Log(true)
			} else {
				t.Error(false)
			}
		}
	}
}

func TestTimeOut(t *testing.T) {
	utp := New(10)
	index, _ := utp.Get()
	t.Log(index.Index)
	time.Sleep(time.Second * 9)
	index, _ = utp.Get()
	t.Log(index.Index)
	time.Sleep(time.Second * 2)
	index, _ = utp.Get()
	t.Log(index.Index)
}

func TestWaitTimeOut(t *testing.T) {
	utp := New(1)
	utp.Get()
	//utp.Put(0)
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	start := time.Now()
	utp.FillQueue()

	go func() {
		//utp.Put(0)
		<-time.After(3 * time.Second)
		utp.Put(0)
		<-time.After(4 * time.Second)
		utp.Put(0)
	}()
	tk2, err := utp.GetTokenFromWaitQueue(ctx)
	fmt.Println(tk2, err, time.Now().Sub(start), 2)
	tk3, err := utp.GetTokenFromWaitQueue(ctx)

	fmt.Println(tk3, err, time.Now().Sub(start), 3)
}
