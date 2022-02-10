/**
 * @Description: 分片下载器实现，用于管理分片下载任务
 * @Author: LuYa 2021-04-29
 */
package shardDownloader

import (
	"context"
	//"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTHost/client"
	"github.com/yottachain/YTHost/clientStore"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//var elkClt = util.NewElkClient("rebuild", &config.Gconfig.ElkReport2)

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
	statistics.DefaultRebuildCount.IncConShard()
	defer statistics.DefaultRebuildCount.DecConShard()

	clt, err := d.cs.GetByAddrString(ctx, nodeId, addr)
	log.Println("[recover_debugtime]  E2_2_0_0 requestShard GetByAddrString nodeid=",nodeId,"addr=",addr,"hash=",base58.Encode(shardID),"error:",err)

	if err != nil {
		statistics.DefaultRebuildCount.IncFailConn()
		return nil, err
	}
	// 连接成功计数
	statistics.DefaultRebuildCount.IncSuccConn()
	//statistics.DefaultRebuildCount.IncSendTokReq()
	//tkString, err := d.GetToken(ctx, clt)
	//if err != nil {
	//	statistics.DefaultRebuildCount.IncFailToken()
	//	return nil, err
	//}
	//// 获取分片token成功计数
	//statistics.DefaultRebuildCount.IncSuccToken()

	var msg message.DownloadShardRequest
	msg.VHF = shardID
	//msg.AllocId = tkString
	buf, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}

	//resBuf, err := clt.SendMsgClose(ctx, message.MsgIDDownloadShardRequest.Value(), buf)
	resBuf, err := clt.SendMsg(ctx, message.MsgIDDownloadShardRequest.Value(), buf)
	log.Println("[recover_debugtime]  E2_2_0_1 requestShard SendMsgClose nodeid=",
		nodeId,"addr=", addr, "hash=", base58.Encode(shardID), "error:", err)

	if err != nil {
		if strings.Contains(err.Error(), "Get data Slice fail") {
			statistics.DefaultRebuildCount.IncFailShard()
		} else {
			statistics.DefaultRebuildCount.IncFailSendShard()
		}

		return nil, err
	}
	if len(resBuf) < 3 {
		return nil, fmt.Errorf("shard len < 3")
	}

	var resMsg message.DownloadShardResponse
	err = proto.Unmarshal(resBuf[2:], &resMsg)
	if err != nil {
		return nil, err
	}
	log.Println("[recover_debugtime]  E2_2_0_1 requestShard Unmarshal nodeid=",
		nodeId, "addr=", addr, "hash=", base58.Encode(shardID), "error:", err)

	// 获取分片成功计数
	statistics.DefaultRebuildCount.IncSuccShard()
	return resMsg.Data, nil
}

func (d *downloader) GetToken(ctx context.Context, clt *client.YTHostClient) (string, error) {

	var getTokenRequestMsg message.NodeCapacityRequest
	getTokenRequestMsg.RequestMsgID = message.MsgIDMultiTaskDescription.Value() + 1
	buf, err := proto.Marshal(&getTokenRequestMsg)
	if err != nil {
		return "", err
	}
	resBuf, err := clt.SendMsg(ctx, message.MsgIDNodeCapacityRequest.Value(), buf)
	if err != nil {
		return "", err
	}

	var resMsg message.NodeCapacityResponse
	err = proto.Unmarshal(resBuf[2:], &resMsg)
	if err != nil {
		return "", err
	}

	return resMsg.AllocId, nil
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
	errChan := make(chan error, 1)

	// @TODO 异步执行下载
	d.q <- struct{}{}
	go func() {
		log.Println("[recover_debugtime]  E2_2_0 goroutine start nodeid=", nodeId,
			"addr=", addr, "hash=", base58.Encode(shardID))
		atomic.AddInt32(&d.stat.Downloading, 1)
		atomic.AddInt32(&d.stat.Total, 1)
		defer func() {
			atomic.AddInt32(&d.Downloading, -1)
			<-d.q
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
		defer cancel()

		resBuf, err := d.requestShard(ctx, nodeId, addr, shardID)
		log.Println("[recover_debugtime]  E2_2_0 requestShard  nodeid=",
			nodeId, "addr=", addr, "hash=", base58.Encode(shardID), "error:", err)

		if err != nil {
			atomic.AddInt32(&d.stat.Error, 1)
			errChan <- err
			return
		}
		atomic.AddInt32(&d.stat.Success, 1)

		d.taskRes.Store(IDString, &shardChan)
		shardChan <- resBuf
		log.Println("[recover_debugtime]  E2_2_0 goroutine end nodeid=",nodeId,"addr=",addr,"hash=",base58.Encode(shardID))
	}()

	return &downloadWait{shardChan: &shardChan, errChan: &errChan}, nil
}

func (d downloader) GetShards(shardList ...[]byte) [][]byte {
	panic("implement me")
}

/**
 * @Description: 下载分片的装载器，用于获取已下载分片，目前存在内存中之后将存在硬盘中
 */
type downloadWait struct {
	shardChan *chan []byte
	errChan   *chan error
}

func (d *downloadWait) Get(ctx context.Context) ([]byte, error) {
	select {
	case err := <-*d.errChan:
		return nil, err
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

	//go func() {
	//	for {
	//		<-time.After(time.Second * 10)
	//		d.stat.Print()
	//	}
	//}()
	return d
}
