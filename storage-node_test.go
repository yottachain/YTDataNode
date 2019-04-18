package node

import "testing"

func TestNewNode(t *testing.T) {
	nd, err := NewStorageNode()
	if err != nil {
		t.Error(err)
	}
	t.Log(nd.Host().ID())
}
