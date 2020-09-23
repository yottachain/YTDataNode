package TaskPool

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestUploadTaskPool_Check(t *testing.T) {
	go Utp().FillToken()
	//go Dtp().FillToken()
	//go func() {
	//	for {
	//		time.Sleep(time.Second * 5)
	//		Utp().MakeTokenQueue()
	//	}
	//}()
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

				go func() {
					ctx, _ := context.WithTimeout(context.Background(), time.Second)
					_, err := Utp().Get(ctx, peer.ID("111"), 0)
					if err != nil {
						atomic.AddInt64(&errNum, 1)
						fmt.Println(err.Error())
					} else {
						atomic.AddInt64(&num, 1)
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
