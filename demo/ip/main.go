package main

import (
	"fmt"
	"github.com/yottachain/YTDataNode/util"
)

func main() {
	pk := "5JRdf22VGxtySBEi4nDoQkvq7pnS3b6AgKhSmLzHCt1C9D8LPNB"
	pubkey, _ := util.GetPublicKey(pk)
	id, _ := util.IdFromPublicKey(pubkey)
	fmt.Println(id)
}
