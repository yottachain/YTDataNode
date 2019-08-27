package uploadTaskPool

import (
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
