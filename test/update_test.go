package test

import (
	"testing"
	"time"
)

func TestGetPath(t *testing.T) {
	var t1 time.Time
	t2 := time.Time{}
	t.Log(t1.Unix() == t2.Unix())
}
