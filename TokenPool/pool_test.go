package TokenPool

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestUploadTaskPool_Check(t *testing.T) {

	go Utp().FillToken()
	go Dtp().FillToken()
	go func() {
		for {
			time.Sleep(time.Second * 2)
			Dtp().ChangeTKFillInterval(time.Millisecond*2 - (time.Millisecond * 2 / 5))
		}
	}()
	var num int64
	var errNum int64
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				fmt.Println(num, ",", errNum, ",", int64(time.Second/Utp().FillTokenInterval)*10)
				os.Exit(0)
				return
			default:
				//time.Sleep(time.Millisecond)
				go func() {
					ctx, _ := context.WithTimeout(context.Background(), time.Second)
					level := int32(rand.Intn(10))
					tk, err := Dtp().Get(ctx, peer.ID("111"), level)
					if err != nil {
						atomic.AddInt64(&errNum, 1)
						//fmt.Println(err.Error())
					} else {
						atomic.AddInt64(&num, 1)
						fmt.Println(tk, level)
					}
				}()
			}
		}
	}(ctx)

	//for {
	//	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	//	_, err := Dtp().Get(ctx, peer.ID("222"), 1)
	//
	//	if err != nil {
	//		fmt.Println(err.Error())
	//	} else {
	//		fmt.Println("111")
	//	}
	//}
}

func TestTokenPool_FillToken(t *testing.T) {
	tk := NewToken()
	tk.FillFromString("nATkeWCoV1BMjXuG2foQpouzoj1K2T1hDKnqW42DSCT2QvENTKXzKtnn4og85NKUQYBAvJimEEYpmF5aSWwLGYtbME2YMhj5xij1ogP2iJzmvvzVvfS21FsWXM4SdtgMx2dww9VnKxkoELKLiaXsFqFyVbAfKkcBPvckD4La6a3QzZdeG42kvguLr1rr9ZDfPUtUQNfkQX3mDwBAyNmY3uvyPnQjvL579UHWHmay8sy79zLF")

	fmt.Println(tk.Tm.Format("2006-01-02 03:04:05"))
}
