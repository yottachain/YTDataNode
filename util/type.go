package util

import (
	"github.com/mr-tron/base58"
	"strings"
)

func IDS2String(ids [][]byte) string {
	var arr []string
	for _, v := range ids {
		arr = append(arr, base58.Encode(v))
	}
	return strings.Join(arr, ",")
}
