package snapi

import (
	"context"
	"fmt"
	pb "git.yottachain.net/snteam/yt-api-server/proto/rebuildapi/rebuildapi.pb.go"
	"github.com/mr-tron/base58"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"google.golang.org/grpc"
	"strconv"
	"sync"
)

var SnApiSerConnFd *grpc.ClientConn

func SnApiConnInit() *grpc.ClientConn {
	if SnApiSerConnFd == nil {
		var url = "10.0.26.177:30909"
		if config.Gconfig.SnApiServerUrl != "" {
			url = config.Gconfig.SnApiServerUrl
		}

		log.Printf("sn api server url is %s\n", url)

		// 连接
		var err error
		SnApiSerConnFd, err = grpc.Dial(url, grpc.WithInsecure())
		if err != nil {
			log.Printf("sn api server dial error %s\n", err.Error())
			return nil
		}
	}
	return SnApiSerConnFd
}

func SendRebuildShardToSnApi(elkData *pb.NodeRebuildRequest) {
	// 连接
	if nil == SnApiConnInit() {
		log.Printf("sn api server dial fail!\n")
		return
	}

	log.Printf("SendRebuildShardToSnApi dial success\n")

	// 初始化客户端
	c := pb.NewReBuildApiClient(SnApiSerConnFd)

	res, err := c.SendNodeRebuild(context.Background(), elkData)
	if err != nil {
		log.Printf("SendRebuildShardToSnApi send data to sn api err %s\n", err.Error())
	}

	if res != nil {
		fmt.Printf("SendRebuildShardToSnApi:%v\n", res.Message)
	}
}

func SendToSnApi(data *message.SelfVerifyResp, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	// 连接
	if nil == SnApiConnInit() {
		log.Printf("sn api server dial fail!\n")
		return
	}

	log.Printf("SendToSnApi dial success\n")

	// 初始化客户端
	c := pb.NewReBuildApiClient(SnApiSerConnFd)

	var elkData pb.NodeRebuildRequest

	//var elkData VerifyErrShards
	id, _ := strconv.ParseInt(data.Id, 10, 64)
	ErrNums, _ := strconv.ParseInt(data.ErrNum, 10, 32)
	elkData.MinerId = id
	elkData.ErrNums = int32(ErrNums)
	for _, v := range data.ErrShard {
		if v.HdbExist {
			continue
		}
		var errShard pb.ErrShard
		errShard.RebuildStatus = 0
		errShard.Shard = base58.Encode(v.DBhash)
		errShard.ShardId = v.Hid
		elkData.ErrShards = append(elkData.ErrShards, &errShard)
	}

	res, err := c.SendNodeRebuild(context.Background(), &elkData)
	if err != nil {
		log.Printf("send data to sn api err %s\n", err.Error())
	}

	if res != nil {
		fmt.Printf("SendNodeRebuild:%v\n", res.Message)
	}
}
