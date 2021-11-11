package util

import (
	"testing"
)

func TestGetEnvField(t *testing.T) {
	v := DefaultLocalEnv.GetFieldValue("ytfs_path")
	if v == "1" {
		t.Log("success")
	} else {
		t.Error("error")
	}
}
