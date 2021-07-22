package util

import (
	"bufio"
	"os"
	"path"
	"strings"
)

var DefaultLocalEnv LocalEnv = ReadLocalEnv()

type EnvKV struct {
	Key   string
	Value string
}
type LocalEnv []EnvKV

func ReadLocalEnv() LocalEnv {
	var localEnv = make(LocalEnv, 0)
	var execPath, err = os.Executable()
	if err != nil {
		return localEnv
	}
	var envFileName = path.Join(path.Dir(execPath), ".env")
	fs, err := os.OpenFile(envFileName, os.O_RDONLY, 0644)
	defer fs.Close()

	if err == nil {
		sc := bufio.NewScanner(fs)
		for sc.Scan() {
			line := sc.Text()
			line = strings.ReplaceAll(line, " ", "")
			line = strings.ReplaceAll(line, "\n", "")
			kv := strings.Split(line, "=")
			if len(kv) < 2 {
				continue
			}
			localEnv = append(localEnv, EnvKV{kv[0], kv[1]})
		}
	}

	return localEnv
}

func (le LocalEnv) GetFieldValue(field string) string {
	var value = ""
	for _, item := range le {
		if item.Key == field {
			value = item.Value
		}
	}
	return value
}
