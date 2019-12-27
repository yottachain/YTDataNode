package node

import "testing"

const (
	id1 = "5J1c7WCgjLLXieWmfRujQeSHebhFzoDKEV5UWd3nep5MDoFP3bV"
	id2 = "5J7sk7AEb1TjwSoiM513feFPmU341sPCr7gHbFHwfUYkx4KPqaz"
)

func TestNewNode(t *testing.T) {
	nd, err := NewStorageNode(id1)
	if err != nil {
		t.Error(err)
	}
	t.Log(nd.Host().ID())
}

func TestUpload(t *testing.T) {
	nd, err := NewStorageNode(id1)
	if err != nil {
		t.Error(err)
	}
	t.Log(nd.Host().ID())
}
