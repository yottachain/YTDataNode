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
	tb := NewTokenBucket(1000, 1*time.Second)
	//for {
	//	tk, _ := utp.GetTokenFromWaitQueue(context.Background())
	//	//utp.Put(tk)
	//	fmt.Println(utp.Check(tk))
	//}
	tk := tb.Get(context.Background())
	//<-time.After(4 * time.Second)
	t.Log(tb.Check(tk))
	t.Log(tb.Check(tk))
	//<-time.After(10 * time.Second)
	//t.Log(tb.Check(tk))
}

func TestTime(t *testing.T) {
	for {
		fmt.Println(time.Time{}.Unix())
		<-time.After(time.Second * 3)
	}
}
