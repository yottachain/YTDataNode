package statistics

import (
	"context"
	"encoding/json"
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

func TestMyTime_MarshalJSON(t *testing.T) {
	tm := &MyTime{time.Now()}
	buf, err := json.Marshal(tm)
	if err == nil {
		fmt.Println(string(buf))
	}
}
