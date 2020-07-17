package config

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestGConfig_Get(t *testing.T) {
	gc := NewGConfig()
	//err := gc.Get()
	//if err != nil {
	//	t.Fatal(err.Error())
	//}
	//t.Log(gc.MaxConn, gc.TokenInterval)
	buf, err := json.Marshal(gc.Gcfg)
	fmt.Println(string(buf), err)
}
