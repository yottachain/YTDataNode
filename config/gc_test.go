package config

import (
	"context"
	"testing"
	"time"
)

func TestGConfig_Get(t *testing.T) {

}

func TestGConfig_UpdateServicet(t *testing.T) {
	Gconfig.UpdateService(context.Background(), time.Second*3)
	select {}
}
