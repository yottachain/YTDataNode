package util

import "io"

func Readline(in io.Reader, max int) ([]byte, error) {
	if max <= 0 {
		max = 1024
	}
	buf := make([]byte, max)
	for {
		n, error := in.Read(buf)
		if error != nil {
			return nil, error
		}
		if n > 0 {
			return buf[:n], nil
		}
	}
}

func ReadStringLine(in io.Reader, max int) string {
	buf, _ := Readline(in, max)
	return string(buf)
}
