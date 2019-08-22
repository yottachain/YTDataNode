package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	var buf []byte

	var nn int
	flag.IntVar(&nn, "n", -1, "")
	flag.Parse()
	if nn < 0 {
		buf = make([]byte, 10)
	} else {
		buf = make([]byte, nn)
	}
	for {
		var num int
		var str string
		if n, err := os.Stdin.Read(buf); err == nil && n > 0 {
			for _, v := range buf {
				str += fmt.Sprintf("%d", v%10)
			}
			num += n
		}
		fmt.Println(str)
		if nn >= 0 && num >= nn {
			break
		}
	}

}
