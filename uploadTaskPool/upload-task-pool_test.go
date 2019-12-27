package uploadTaskPool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewTokenFromString(t *testing.T) {
	tk, _ := NewTokenFromString("As5KQYFqzvsushMvPjrxTfc3CUiZyXXbRiR3H5zm9vYwbWh32BxofN9PGVWpw6KYTn6Nrd227V2cE1QuD4BP4iV1tt5zFyqVgrmkrhUTziG2z4VwkHhvY3RVkz7rtMUe79suAJBCRSnRLFSzps9XdRoLPGYtfA4TUu7nRUFy")
	t.Log(tk.Tm)
}

func TestUploadTaskPool_Check(t *testing.T) {
	utp := New(10, 10*time.Second, 1*time.Millisecond)
	utp.FillQueue()
	//for {
	//	tk, _ := utp.GetTokenFromWaitQueue(context.Background())
	//	//utp.Put(tk)
	//	fmt.Println(utp.Check(tk))
	//}
	tk, _ := utp.GetTokenFromWaitQueue(context.Background())
	<-time.After(1 * time.Second)
	t.Log(utp.Check(tk))
	<-time.After(10 * time.Second)
	t.Log(utp.Check(tk))
}

func TestTime(t *testing.T) {
	for {
		fmt.Println(time.Time{}.Unix())
		<-time.After(time.Second * 3)
	}
}
