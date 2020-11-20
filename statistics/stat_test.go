package statistics

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestStat_AddSaveRequestCount(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			<-time.After(time.Millisecond * 100)
		}
	}
}
func TestLastUpTime_Read(t *testing.T) {
	fmt.Println((&LastUpTime{}).Read())
}
