/**
 * @Description: LRC执行器的实现
 * @Author: LuYa 2021-04-28
 */
package actuator

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTDataNode/activeNodeList"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/recover/shardDownloader"
	"github.com/yottachain/YTDataNode/statistics"
	lrc "github.com/yottachain/YTLRC"
	"strings"
	"sync"
	"time"
)

type Shard struct {
	Index int16
	Data  []byte
}

type shardsMap struct {
	shards map[string]*Shard
	mutex  sync.Mutex
}

func (s *shardsMap) Len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return len(s.shards)
}

func (s *shardsMap) Set(key string, data *Shard) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.shards[key] = data
}

func (s *shardsMap) Get(key string) (data *Shard, ok bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, ok = s.shards[key]
	return
}

func (s *shardsMap) GetMap() map[string]*Shard {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.shards
}

func (s shardsMap) Init() *shardsMap {
	s.shards = make(map[string]*Shard)
	return &s
}

/**
 * @Description: LRC执行器
 */
type LRCTaskActuator struct {
	downloader shardDownloader.ShardDownloader // 下载器
	lrcHandler *lrc.Shardsinfo                 // lrc句柄
	msg        message.TaskDescription         // 重建消息
	shards     *shardsMap                      // 重建所需分片
	opts       Options
}

/**
 * @Description: 初始化LRC
 * @receiver L
 * @return error
 */
func (L *LRCTaskActuator) initLRCHandler(stage RecoverStage) error {
	l := &lrc.Shardsinfo{}

	l.OriginalCount = uint16(len(L.msg.Hashs) - int(L.msg.ParityShardCount))
	l.RecoverNum = 13
	l.Lostindex = uint16(L.msg.RecoverId)
	l.GetRCHandle(l)
	L.lrcHandler = l

	log.Println("[recover]", "执行阶段", stage)

	L.shards = shardsMap{}.Init()

	return nil
}

/**
 * @Description: 获取所重建需分片索引列表
 * @receiver L
 * @return []int16 分片索引
 * @return error
 */
func (L *LRCTaskActuator) getNeedShardList() ([]int16, error) {
	if L.lrcHandler == nil {
		return nil, fmt.Errorf("lrc handler is nil")
	}

	L.lrcHandler.SetHandleParam(L.lrcHandler.Handle, uint8(L.msg.RecoverId), uint8(L.opts.Stage-1))

	indexes := make([]int16, 0)

	//// @TODO 如果已经有部分分片下载成功了则只检查未下载成功分片
	if L.shards.Len() > 0 {
		for _, shard := range L.shards.GetMap() {
			if shard.Data == nil {
				indexes = append(indexes, shard.Index)
			}
		}
		return indexes, nil
	}

	// @TODO 当没有已经缓存的分片时从LRC获取需要分片列表
	needList, _ := L.lrcHandler.GetNeededShardList(L.lrcHandler.Handle)

	for curr := needList.Front(); curr != nil; curr = curr.Next() {
		if index, ok := curr.Value.(int16); ok {
			indexes = append(indexes, index)
		} else {
			return nil, fmt.Errorf("get need shard list fail")
		}
	}

	return indexes, nil
}

/**
 * @Description: 添加下载任务
 * @receiver L
 * @params duration 每个任务最大等待时间
 * @param indexes 下载任务分片索引
 */
func (L *LRCTaskActuator) addDownloadTask(duration time.Duration, indexes ...int16) (*sync.WaitGroup, error) {

	wg := &sync.WaitGroup{}
	wg.Add(len(indexes))
	// @TODO 循环添加下载任务
	for _, shardIndex := range indexes {
		// 中断循环开关
		var brk = false
		if brk {
			break
		}

		addrInfo := L.msg.Locations[shardIndex]
		hash := L.msg.Hashs[shardIndex]

		//  下载分片计数加一
		statistics.DefaultRebuildCount.IncShardForRbd()
		d, err := L.downloader.AddTask(addrInfo.NodeId, addrInfo.Addrs, hash)
		if err != nil {
			return nil, fmt.Errorf("add download task fail %s", err.Error())
		}

		// @TODO 异步等待下载任务执行完成
		func(key []byte, d shardDownloader.DownloaderWait, index int16) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			shard, err := d.Get(ctx)
			// @TODO 如果因为分片不存在导致错误，直接中断
			if err != nil && strings.Contains(err.Error(), "Get data Slice fail") {
				brk = true
			}

			L.shards.Set(hex.EncodeToString(key), &Shard{
				Index: shardIndex,
				Data:  shard,
			})

		}(hash, d, shardIndex)
	}

	return wg, nil
}

/**
 * @Description: 检查重建所需分片是否存在
 * @receiver L
 * @return ok
 * @return lossShard 丢失分片
 */
func (L *LRCTaskActuator) checkNeedShardsExist() (ok bool) {
	for _, v := range L.shards.GetMap() {
		if v.Data == nil {
			return false
		}
	}
	return true
}

/**
 * @Description: 下载分片循环
 * @receiver L
 * @param ctx
 * @return error
 */
func (L *LRCTaskActuator) downloadLoop(ctx context.Context) error {
	var errCount uint64

start:
	if errCount > 30 {
		return nil
	}
	errCount++

	select {
	case <-ctx.Done():
		return fmt.Errorf("download loop time out")
	default:
		needShardIndexes, err := L.getNeedShardList()
		log.Println("需要分片", needShardIndexes, "任务", hex.EncodeToString(L.msg.Id), "尝试", errCount)
		downloadTask, err := L.addDownloadTask(time.Minute*5, needShardIndexes...)
		if err != nil {
			return err
		}
		downloadTask.Wait()

		if L.shards.Len() < len(needShardIndexes) {
			goto start
		}
		if ok := L.checkNeedShardsExist(); !ok {
			// @TODO 如果检查分片不足跳回开头继续下载
			goto start
		}
	}

	return nil
}

/**
 * @Description: 预判重建能否成功
 * @receiver L
 * @return ok
 */
func (L *LRCTaskActuator) preJudge() (ok bool) {

	indexes, err := L.getNeedShardList()
	if err != nil {
		return false
	}

	var onLineShardIndexes = make([]int16, 0)
	for _, index := range indexes {
		if ok := activeNodeList.HasNodeid(L.msg.Locations[index].NodeId); ok {
			onLineShardIndexes = append(onLineShardIndexes, index)
		} else {
			// 如果是行列校验，所需分片必须都在线
			if L.opts.Stage < 3 {
				return false
			}
		}
	}

	// 如果是全局校验填充假数据，尝试校验
	if L.opts.Stage >= 3 {
		for _, index := range onLineShardIndexes {
			// 填充假数据
			L.lrcHandler.ShardExist[index] = 1

			if status, _ := L.lrcHandler.AddShardData(L.lrcHandler.Handle, TestData[index][:]); status > 0 {
				return true
			}
		}
		return false
	}

	return true
}

func (L *LRCTaskActuator) backupTask() ([]byte, error) {
	if L.msg.BackupLocation == nil {
		return nil, fmt.Errorf("no backup")
	}
	if !activeNodeList.HasNodeid(L.msg.BackupLocation.NodeId) {
		return nil, fmt.Errorf("backup is offline")
	}

	for i := 0; i < 5; i++ {
		dw, err := L.downloader.AddTask(L.msg.BackupLocation.NodeId, L.msg.BackupLocation.Addrs, L.msg.Hashs[L.msg.RecoverId])
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		data, err := dw.Get(ctx)
		if err != nil {
			continue
		}
		log.Printf("%s 从备份恢复成功 %d\n", hex.EncodeToString(L.msg.Hashs[L.msg.RecoverId]), i)
		return data, nil
	}

	return nil, fmt.Errorf("download backup fail")
}

/**
 * @Description: 恢复数据
 * @receiver L
 * @return []byte
 * @return error
 */
func (L *LRCTaskActuator) recoverShard() ([]byte, error) {
	L.lrcHandler.SetHandleParam(L.lrcHandler.Handle, uint8(L.msg.RecoverId), uint8(1))
	for _, v := range L.shards.GetMap() {
		status, _ := L.lrcHandler.AddShardData(L.lrcHandler.Handle, v.Data)
		_, stage, _ := L.lrcHandler.GetHandleParam(L.lrcHandler.Handle)
		fmt.Println("add shard", stage, v.Index, "status", status, "任务", hex.EncodeToString(L.msg.Id))
		if status > 0 {
			data, status := L.lrcHandler.GetRebuildData(L.lrcHandler)
			if data == nil {
				return nil, fmt.Errorf("recover data fail,status: %d", status)
			}
			return data, nil
		} else if status < 0 {
			hash := md5.Sum(v.Data)
			fmt.Println(hex.EncodeToString(L.msg.Id), "添加分片失败", status, "分片数据hash", hex.EncodeToString(hash[:]), v.Data)
		}
	}

	buf := bytes.NewBuffer([]byte{})
	for _, v := range L.shards.GetMap() {
		hash := md5.Sum(v.Data)
		fmt.Fprintln(
			buf,
			"恢复失败 已添加",
			fmt.Sprintf(" %d 分片hash %s 原 hash %s 任务 %s",
				v.Index,
				hex.EncodeToString(hash[:]),
				hex.EncodeToString(L.msg.Hashs[v.Index]),
				hex.EncodeToString(L.msg.Id),
			))
	}
	log.Println(buf.String())
	return nil, fmt.Errorf("缺少分片")
}

/**
 * @Description: 验证重建出来的数据的HASH
 * @receiver L
 * @param recoverData
 * @return error
 */
func (L *LRCTaskActuator) verifyLRCRecoveredData(recoverData []byte) error {
	hashBytes := md5.Sum(recoverData)
	hash := hashBytes[:]

	if !bytes.Equal(hash, L.msg.Hashs[L.msg.RecoverId]) {
		return fmt.Errorf("recovered data hash verify fail %s %s", hex.EncodeToString(hash), hex.EncodeToString(L.msg.Hashs[L.msg.RecoverId]))
	}
	return nil
}

/**
 * @Description: 解析任务消息
 * @receiver L
 * @param msgData
 * @return error
 */
func (L *LRCTaskActuator) parseMsgData(msgData []byte) error {
	var msg message.TaskDescription
	err := proto.UnmarshalMerge(msgData, &msg)
	if err != nil {
		return err
	}
	L.msg = msg
	return nil
}

/**
 * @Description: 执行重建任务
 * @receiver L
 * @param msg
 * @param opts
 * @return err
 */
func (L *LRCTaskActuator) ExecTask(msgData []byte, opts Options) (data []byte, msgID []byte, err error) {

	L.opts = opts
	err = L.parseMsgData(msgData)
	msgID = L.msg.Id
	if err != nil {
		log.Println("[recover] exectask error:",err.Error())
		return
	}

	// @TODO 如果是备份恢复阶段，直接执行备份恢复
	if L.opts.Stage == 0 {
		data, err = L.backupTask()
		log.Println("[recover] backupTask error:",err)
		return
	}

	// @TODO 初始化LRC句柄
	err = L.initLRCHandler(opts.Stage)
	if err != nil {
		log.Println("[recover] initLRCHandler error:",err.Error())
		return
	}

	// @TODO 预判
	if ok := L.preJudge(); !ok {
		err = fmt.Errorf("预判失败 阶段 %d任务 %s", L.opts.Stage, hex.EncodeToString(L.msg.Id))
		log.Println("[recover] preJudge error:", err.Error())
		return
	}
	// 成功预判计数加一
	statistics.DefaultRebuildCount.IncPassJudge()

	// @TODO 下载分片
	ctx, cancel := context.WithDeadline(context.Background(), opts.Expired)
	defer cancel()
	err = L.downloadLoop(ctx)
	if err != nil {
		return
	}

	// @TODO LRC恢复
	recoverData, err := L.recoverShard()
	if err != nil {
		return
	}

	// @TODO 验证数据
	if err = L.verifyLRCRecoveredData(recoverData); err != nil {
		return
	}

	data = recoverData
	// 成功重建计数加一
	statistics.DefaultRebuildCount.IncSuccRbd()
	return
}

func (L *LRCTaskActuator) Free() {
	L.lrcHandler.FreeHandle()
	L.lrcHandler.FreeRecoverData()
}

func New(downloader shardDownloader.ShardDownloader) *LRCTaskActuator {
	return &LRCTaskActuator{
		downloader: downloader,
		lrcHandler: nil,
		msg:        message.TaskDescription{},
	}
}
