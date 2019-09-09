package uploadTaskPool

import (
	"context"
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

func TestToken(t *testing.T) {
	tk := Token{}
	tk.FillFromString("As5KQYFqzvsushMvPjrxTfc3CUip3GDxqH5ohawXitv6BaLAjNje8L6ZbVZhwtk9cND6oH5sYc8WW1qAZdHNC7eP9bmMnL8JwQcuxp3RekQpaXhjHRXA4midZB8CoLZsZv992QsvgcbMSgAyMcueBShpEfd4HGeDyN6prvoh")
	t.Log(tk.Tm.String(), tk.Index, tk.UUID)
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
	utp.FillQueue()
	outTk, err := utp.GetTokenFromWaitQueue(context.Background())
	if err != nil {
		t.Error(err)
	}
	<-time.After(time.Second * 11)
	if !utp.Check(outTk) {
		t.Log("pass")
	} else {
		t.Error("unpass")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tk, err := utp.GetTokenFromWaitQueue(ctx)
	if err != nil {
		t.Error(err)
	}
	<-time.After(3 * time.Second)
	if utp.Check(tk) {
		t.Log("pass")
	} else {
		t.Error("unpass")
	}
}
