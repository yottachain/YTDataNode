/**
 * @Description: 分片下载器实现，用于管理分片下载任务
 * @Author: LuYa 2021-04-29
 */
package shardDownloader

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/gogo/protobuf/proto"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTHost/clientStore"
	"sync"
	"sync/atomic"
	"time"
)

type stat struct {
	Downloading int32
	Success     int32
	Error       int32
	Total       int32
}

func (s *stat) Print() {
	fmt.Printf(
		"downloading: %d success: %d error: %d total: %d\n",
		atomic.LoadInt32(&s.Downloading),
		atomic.LoadInt32(&s.Success),
		atomic.LoadInt32(&s.Error),
		atomic.LoadInt32(&s.Total),
	)
}

type downloader struct {
	cs      *clientStore.ClientStore
	taskRes sync.Map
	q       chan struct{}
	stat
}

/**
 * @Description: 请求远程节点下载分片
 * @receiver d
 * @param ctx
 * @param nodeId
 * @param addr
 * @param shardID
 * @return []byte
 * @return error
 */
func (d *downloader) requestShard(ctx context.Context, nodeId string, addr []string, shardID []byte) ([]byte, error) {

	clt, err := d.cs.GetByAddrString(ctx, nodeId, addr)
	if err != nil {
		return nil, err
	}

	var msg message.DownloadShardRequest
	msg.VHF = shardID
	buf, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}

	resBuf, err := clt.SendMsg(ctx, message.MsgIDDownloadShardRequest.Value(), buf)
	if err != nil {
		log.Println("下载失败", err)
		return nil, err
	}
	if len(resBuf) < 3 {
		return nil, fmt.Errorf("shard len <3")
	}

	var resMsg message.DownloadShardResponse
	err = proto.Unmarshal(resBuf[2:], &resMsg)
	if err != nil {
		log.Println("下载失败", err)
		return nil, err
	}

	return resMsg.Data, nil
}

/**
 * @Description: 添加下载分片任务
 * @receiver d
 * @param nodeId
 * @param addr
 * @param shardID
 * @return DownloaderWait
 * @return error
 */
func (d *downloader) AddTask(nodeId string, addr []string, shardID []byte) (DownloaderWait, error) {
	IDString := hex.EncodeToString(shardID)
	shardChan := make(chan []byte, 1)

	// @TODO 异步执行下载
	d.q <- struct{}{}
	go func() {
		atomic.AddInt32(&d.stat.Downloading, 1)
		atomic.AddInt32(&d.stat.Total, 1)
		defer func() {
			atomic.AddInt32(&d.Downloading, -1)
			<-d.q
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		resBuf, err := d.requestShard(ctx, nodeId, addr, shardID)
		if err != nil {
			atomic.AddInt32(&d.stat.Error, 1)
			shardChan <- nil
			return
		}
		atomic.AddInt32(&d.stat.Success, 1)

		d.taskRes.Store(IDString, &shardChan)
		shardChan <- resBuf
	}()

	return &downloadWait{shardChan: &shardChan}, nil
}

func (d downloader) GetShards(shardList ...[]byte) [][]byte {
	panic("implement me")
}

/**
 * @Description: 下载分片的装载器，用于获取已下载分片，目前存在内存中之后将存在硬盘中
 */
type downloadWait struct {
	shardChan *chan []byte
}

func (d *downloadWait) Get(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("ctx done")
	case shard := <-*d.shardChan:
		if shard == nil {
			return nil, fmt.Errorf("download shard error")
		}
		return shard, nil
	}
}

func New(store *clientStore.ClientStore, max int) *downloader {
	d := new(downloader)
	d.cs = store
	d.q = make(chan struct{}, max)
	//d.taskRes = sync.Map{}

	go func() {
		for {
			<-time.After(time.Second * 10)
			d.stat.Print()
		}
	}()
	return d
}
