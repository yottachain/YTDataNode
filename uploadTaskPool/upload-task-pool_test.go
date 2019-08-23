package uploadTaskPool

import "testing"

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
	for i := 0; i < 10; i++ {
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
}

func TestCheckToken(t *testing.T) {
	utp := New(10)
	for i := 0; i < 10; i++ {
		tk, err := utp.Get()
		if err != nil {
			t.Error(err)
		} else {
			t.Log(tk.String())
			ntk := Token{}

			err = ntk.FillFromString(tk.String())
			if err != nil {
				t.Error(err)
			}
			t.Log(tk.index, tk.UUID.String(), ntk.UUID.String())
			t.Log(utp.Check(&ntk))
		}
	}
}
