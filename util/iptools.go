package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func GetSelfIP() string {
	resp, err := http.Get("http://123.57.81.177/self-ip")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s", buf)
}
