package main

import (
	"fmt"
	"github.com/eoscanada/eos-go/ecc"
)

func main() {
	pk, _ := ecc.NewPrivateKey("5JWUif6MMtT8vMqH7GTQup5sMbkPN93zAaSFejm4nWhDHWtJHQi")
	var a [32]byte
	a = [32]byte{1, 1}
	out, err := pk.Sign(a[0:32])
	fmt.Println(out.Content, err)
}
