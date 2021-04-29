package shardDownloader

import (
	"context"
)

type DownloaderWait interface {
	Get(ctx context.Context) ([]byte, error)
}

type ShardDownloader interface {
	AddTask(nodeId string, addr []string, shardID []byte) (DownloaderWait, error)
	GetShards(shardList ...[]byte) [][]byte
}
