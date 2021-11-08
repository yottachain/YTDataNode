package randDownload

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDataNode/Perf"
	"github.com/yottachain/YTDataNode/TokenPool"
	"github.com/yottachain/YTDataNode/activeNodeList"
	"github.com/yottachain/YTDataNode/config"
	"github.com/yottachain/YTDataNode/logBuffer"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/statistics"
	"github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTDataNode/util"
	"github.com/yottachain/YTHost/client"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

//var ctl chan struct{}

var stop = false

var errNoTK = fmt.Errorf("notk")

//var elkClient = util.NewElkClient("XxTest", &config.Gconfig.ElkReport2)

var Sn storageNodeInterface.StorageNode
var wl = activeNodeList.NewWeightNodeList(
	time.Minute*5,
	time.Minute*time.Duration(config.Gconfig.NodeListUpdateTime),
	config.Gconfig.RandDownloadGroupSize,
	util.GetSelfIP())

func GetRandNode() (*peer.AddrInfo, error) {
	nodeList := wl.Get()

	pi := &peer.AddrInfo{}
	nl := len(nodeList)

	var randNode *activeNodeList.Data

	if nl <= 0 {
		return nil, fmt.Errorf("no node")
	}
	randIndex := rand.Intn(nl)
	randNode = nodeList[randIndex]

	id, err := peer.IDB58Decode(randNode.NodeID)
	if err != nil {
		return nil, err
	}
	pi.ID = id
	for _, v := range randNode.IP {
		ma, err := multiaddr.NewMultiaddr(v)
		if err != nil {
			continue
		}
		pi.Addrs = append(pi.Addrs, ma)
	}
	return pi, nil
}

func getTK(clt *client.YTHostClient, msgID int32, ctx context.Context) (string, error) {
	var getTokenMsg message.NodeCapacityRequest
	getTokenMsg.RequestMsgID = msgID
	getTKMsgBuf, err := proto.Marshal(&getTokenMsg)
	if err != nil {
		return "", errNoTK
	}

	getTKResBuf, err := clt.SendMsg(ctx, message.MsgIDNodeCapacityRequest.Value(), getTKMsgBuf)
	if err != nil {
		return "", errNoTK
	}
	var tokenMsg message.NodeCapacityResponse
	err = proto.Unmarshal(getTKResBuf[2:], &tokenMsg)
	if err != nil {
		return "", errNoTK
	}
	return tokenMsg.AllocId, err
}

func GetRandClt() (*client.YTHostClient, error) {
	return nil, nil
}

func UploadFromRandNode(ctx context.Context) error {
	var reportContent = new(ReportContent)
	reportContent.Success = true
	reportContent.TestType = "upload"

	pi, err := GetRandNode()
	if err != nil {
		reportContent.Error = err.Error()
		reportContent.Success = false
		return err
	}
	if Sn == nil {
		return fmt.Errorf("no storage-node")
	}

	statistics.DefaultStat.RXTestConnectRate.AddCount()
	clt, err := Sn.Host().ClientStore().Get(ctx, pi.ID, pi.Addrs)
	if err != nil {
		return err
	}
	statistics.DefaultStat.RXTestConnectRate.AddSuccess()
	defer clt.Close()

	tk, err := getTK(clt, message.MsgIDTestGetBlock.Value()+1, ctx)
	if err != nil {
		reportContent.Error = err.Error()
		reportContent.Success = false
		return err
	}

	var testMsg message.TestGetBlock

	// 第一次发送消息模拟上传
	testMsg.Msg = Perf.MSG_UPLOAD
	testMsg.Pld = make([]byte, 1024*16)
	rand.Read(testMsg.Pld)
	testMsg.AllocID = tk

	testMsgBuf, err := proto.Marshal(&testMsg)
	if err != nil {
		return err
	}
	_, err = clt.SendMsg(ctx, message.MsgIDTestGetBlock.Value(), testMsgBuf)
	if err != nil {
		reportContent.Error = err.Error()
		reportContent.Success = false
		return err
	}
	reportContent.FromID = Sn.Config().ID
	reportContent.ToID = pi.ID.Pretty()

	//if config.Gconfig.ElkReport2 {
	//	elkClient.AddLogAsync(reportContent)
	//}
	return nil
}

type ReportContent struct {
	FromID string
	ToID   string

	TestType string
	Success  bool
	Error    string
}

func DownloadFromRandNode(ctx context.Context) error {
	var err error

	pi, err := GetRandNode()
	if err != nil {
		return err
	}

	if Sn == nil {
		return fmt.Errorf("no storage-node")
	}

	statistics.DefaultStat.TXTestConnectRate.AddCount()
	clt, err := Sn.Host().ClientStore().Get(ctx, pi.ID, pi.Addrs)
	if err != nil {
		return err
	}
	statistics.DefaultStat.TXTestConnectRate.AddSuccess()
	defer clt.Close()
	tk, err := getTK(clt, message.MsgIDTestGetBlock.Value(), ctx)
	if err != nil {
		return err
	}

	var testMsg message.TestGetBlock

	// 第一次发送消息模拟下载
	testMsg.Msg = Perf.MSG_DOWNLOAD
	testMsg.AllocID = tk
	testMsgBuf, err := proto.Marshal(&testMsg)
	if err != nil {
		return err
	}
	_, err = clt.SendMsg(ctx, message.MsgIDTestGetBlock.Value(), testMsgBuf)
	if err != nil {
		return err
	}
	// 第二次发送消息消耗token
	testMsg.Msg = Perf.MSG_CHECKOUT
	testMsgBuf, err = proto.Marshal(&testMsg)
	if err != nil {
		return err
	}
	_, err = clt.SendMsg(ctx, message.MsgIDTestGetBlock.Value(), testMsgBuf)
	if err != nil {
		return err
	}
	return nil
}
func RunRX(RxCtl chan struct{}) {
	var successCount uint64
	var errorCount uint64
	var execChan *chan struct{}
	var count uint64
	rand.Seed(int64(os.Getpid()))

	go func() {
		for {
			<-time.After(time.Minute)
			log.Println("[randUpload] success", successCount, "error", errorCount, "exec", len(*execChan), "count", count)
		}
	}()

	c := make(chan struct{}, int(math.Min(float64(TokenPool.Dtp().GetTFillTKSpeed())/4, float64(config.Gconfig.RXTestNum))))
	execChan = &c

	go func() {
		for {
			c := make(chan struct{}, int(math.Min(float64(TokenPool.Utp().GetTFillTKSpeed())/4, float64(config.Gconfig.RXTestNum))))
			execChan = &c
			<-time.After(5 * time.Minute)
		}
	}()

	times := uint64(0)
	// rx
	for {
		log.Println("[randUpload] loop")
		<- RxCtl
		times++
		if times % 2000 == 0{
			log.Println("[randUpload] RunRX start nowtime:",time.Now())
		}

		if stop {
			log.Println("[randUpload] RunRX stop nowtime:",time.Now())
			<-time.After(time.Minute * 60)
			continue
		}
		if execChan == nil {
			log.Println("[randUpload] execChan is Nil")
			continue
		}
		ec := *execChan
		ec <- struct{}{}
		go func(ec chan struct{}) {
			defer func() {
				<-ec
			}()

			ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(config.Gconfig.TTL))
			defer cancle()

			atomic.AddUint64(&count, 1)
			log.Println("[randUpload] start")
			err := UploadFromRandNode(ctx)
			if err != nil && err.Error() != errNoTK.Error() {
				logBuffer.ErrorLogger.Println(err.Error())
				atomic.AddUint64(&errorCount, 1)
			} else if err == nil {
				atomic.AddUint64(&successCount, 1)
			}

		}(ec)
		<-time.After(time.Millisecond * time.Duration(config.Gconfig.RXTestSleep))
	}
}

func RunTX(TxCtl chan struct{}) {
	var successCount uint64
	var errorCount uint64
	var execChan *chan struct{}
	rand.Seed(int64(os.Getpid()) + time.Now().Unix())

	go func() {
		for {
			<-time.After(time.Minute)
			log.Println("[randDownload] success", successCount, "error", errorCount, "exec", len(*execChan))
		}
	}()

	c := make(chan struct{}, int(math.Min(float64(TokenPool.Utp().GetTFillTKSpeed())/4, float64(config.Gconfig.TXTestNum))))
	execChan = &c

	go func() {
		for {
			c := make(chan struct{}, int(math.Min(float64(TokenPool.Utp().GetTFillTKSpeed())/4, float64(config.Gconfig.TXTestNum))))
			execChan = &c
			<-time.After(5 * time.Minute)
		}
	}()

	times := uint64(0)
	// tx
	for {
		<- TxCtl
		times++
		if times % 2000 == 0{
			log.Println("[randDownload] RunTX start nowtime:",time.Now())
		}
		if stop {
			<-time.After(time.Minute * 60)
			continue
		}
		if execChan == nil {
			continue
		}
		ec := *execChan
		ec <- struct{}{}
		go func(ec chan struct{}) {
			defer func() {
				<-ec
			}()

			ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(config.Gconfig.TTL))
			defer cancle()

			err := DownloadFromRandNode(ctx)
			if err != nil && err.Error() != errNoTK.Error() {
				logBuffer.ErrorLogger.Println(err.Error())
				atomic.AddUint64(&errorCount, 1)
			} else if err == nil {
				atomic.AddUint64(&successCount, 1)
			}

		}(ec)
		<-time.After(time.Millisecond * time.Duration(config.Gconfig.TXTestSleep))
	}
}

func Run() {
	RxCtl := make(chan struct{})
	TxCtl := make(chan struct{})
	go RunCtl(RxCtl,TxCtl)
	go RunRX(RxCtl)
	go RunTX(TxCtl)
}

func RunCtl( RxCtl, TxCtl chan struct{}){
	for{
		start := time.Now()
		for{
			RxCtl <- struct{}{}
			TxCtl <- struct{}{}
			if time.Now().Sub(start).Seconds() >= 3600{
				break
			}
		}
		<-time.After(time.Hour * 11)
	}
}

func Stop() {
	stop = true
}

//func Start(){
//	stop = false
//}
