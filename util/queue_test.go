package util

import "testing"

type tt struct {
	V string
}

func TestQueue_Push(t *testing.T) {
	q := new(Queue)
	d := tt{"111"}
	q.Push(d)
	t.Log(q.size)
	q.Push("121")
	data1, err:=q.Pop()
	if err != nil {
		t.Error(err)
	}
	if data1.(tt).V == "111" {
		t.Log("ok")
	} else {
		t.Error("error",data1)
	}
}
