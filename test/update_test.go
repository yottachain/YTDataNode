package test

import (
	"github.com/yottachain/YTDataNode/commander"
	"testing"
)

func TestGetPath(t *testing.T) {
	file, _ := commander.GetCurrentPath()
	t.Log(file)
}
