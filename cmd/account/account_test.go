package account

import (
	"github.com/eoscanada/eos-go"
	"testing"
)

func TestNewSignedTransaction(t *testing.T) {
	NewSignedTransaction(struct {
		Testfiled string `json:"test" name:"test"`
	}{}, "1", "2", []eos.PermissionLevel{})
}
