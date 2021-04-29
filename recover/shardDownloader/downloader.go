/**
 * @Description: 分片下载器实现，用于管理分片下载任务
 * @Author: LuYa 2021-04-29
 */
package shardDownloader

import (
	"github.com/yottachain/YTHost/clientStore"
)

type downloader struct {
	cs *clientStore.ClientStore
}

func (d downloader) AddTask(nodeId string, addr []string, shardID []byte) (DownloaderWait, error) {
	panic("implement me")
}

func (d downloader) GetShards(shardList ...[]byte) [][]byte {
	panic("implement me")
}
