package util

import (
	"fmt"
	"os"
	"os/user"
	"path"
)

// GetCurrentUserHome 获取当前用户主目录
func GetCurrentUserHome() string {
	var userDir string
	if u, err := user.Current(); err == nil {
		userDir = u.HomeDir
	}
	return userDir
}

// GetYTFSPath 获取YTFS文件存放路径
//
// 如果存在环境变量ytfs_path则使用环境变量ytfs_path
func GetYTFSPath() string {
	ps, ok := os.LookupEnv("ytfs_path")
	if ok {
		return ps
	}
	return GetCurrentUserHome() + "/YTFS"
}

// GetConfigPath 获取当前用户配置文件路径
func GetConfigPath() string {
	return GetYTFSPath() + "/config.json"
}

// PathExists 判断文件是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetLogFile(name string) *os.File {
	file, err := os.OpenFile(path.Join(GetYTFSPath(), name), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		if err != nil {
			fmt.Println("打开日志文件失败")
		}
	}
	return file
}
