package config

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestGConfig_Get(t *testing.T) {
	cfg, _ := ReadConfig()
	gc := NewGConfig(cfg)
	err := gc.Get()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(gc.MaxConn, gc.TokenInterval)
}

func TestGConfig_UpdateServicet(t *testing.T) {
	cfg, _ := ReadConfig()
	gc := NewGConfig(cfg)
	gc.OnUpdate = func(gc Gcfg) {
		fmt.Println("success")
	}
	err := gc.Get()
	if err != nil {
		t.Fatal(err.Error())
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	gc.UpdateService(ctx, time.Millisecond*500)
	t.Log(gc.MaxConn, gc.TokenInterval)
}
