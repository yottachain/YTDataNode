/**
 * @Description: LRC执行器的实现
 * @Author: LuYa 2021-04-28
 */
package actuator

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/recover/shardDownloader"
	lrc "github.com/yottachain/YTLRC"
	"sync"
	"time"
)

/**
 * @Description: LRC执行器
 */
type LRCTaskActuator struct {
	downloader shardDownloader.ShardDownloader // 下载器
	lrcHandler *lrc.Shardsinfo                 // lrc句柄
	msg        message.TaskDescription         // 重建消息
	shards     map[int16][]byte                // 重建所需分片
}

/**
 * @Description: 初始化LRC
 * @receiver L
 * @return error
 */
func (L *LRCTaskActuator) initLRCHandler() error {
	l := &lrc.Shardsinfo{}

	l.OriginalCount = uint16(len(L.msg.Hashs) - int(L.msg.ParityShardCount))
	l.RecoverNum = 13
	l.Lostindex = uint16(L.msg.RecoverId)
	L.lrcHandler = l

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

	indexes := make([]int16, 0)

	// @TODO 如果已经有部分分片下载成功了则只检查未下载成功分片
	if L.shards != nil {
		for index, shard := range L.shards {
			if shard == nil {
				indexes = append(indexes, index)
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
		addrInfo := L.msg.Locations[shardIndex]
		hash := L.msg.Hashs[shardIndex]
		d, err := L.downloader.AddTask(addrInfo.NodeId, addrInfo.Addrs, hash)
		if err != nil {
			return nil, fmt.Errorf("add download task fail")
		}

		// @TODO 移步等待下载任务执行完成
		go func(index int16) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			shard, err := d.Get(ctx)
			if err == nil {
				L.shards[shardIndex] = shard
			}
		}(shardIndex)
	}

	return wg, nil
}

/**
 * @Description: 检查重建所需分片是否存在
 * @receiver L
 * @return ok
 * @return lossShard 丢失分片
 */
func (L *LRCTaskActuator) checkNeedShardsExist() (ok bool, lossShard []int16) {
	panic("impl it")
}

/**
 * @Description: 下载分片循环
 * @receiver L
 * @param ctx
 * @return error
 */
func (L *LRCTaskActuator) downloadLoop(ctx context.Context) error {

start:

	select {
	case <-ctx.Done():
		return fmt.Errorf("download loop time out")
	default:
		needShardIndexes, err := L.getNeedShardList()
		downloadTask, err := L.addDownloadTask(time.Minute, needShardIndexes...)
		if err != nil {
			return err
		}
		downloadTask.Wait()

		if ok, _ := L.checkNeedShardsExist(); !ok {
			// @TODO 如果检查分片不足跳回开头继续下载
			goto start
		}
	}

	return nil
}

/**
 * @Description: 恢复数据
 * @receiver L
 * @return []byte
 * @return error
 */
func (L *LRCTaskActuator) recoverShard() ([]byte, error) {
	for _, v := range L.shards {
		_, err := L.lrcHandler.AddShardData(L.lrcHandler.Handle, v)
		if err != nil {
			return nil, err
		}
	}

	data, status := L.lrcHandler.GetRebuildData(L.lrcHandler)
	if data == nil {
		return nil, fmt.Errorf("recover data fail,status: %d", status)
	}
	return data, nil
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
		return fmt.Errorf("recovered data hash verify fail")
	}
	return nil
}

/**
 * @Description: 执行重建任务
 * @receiver L
 * @param msg
 * @param opts
 * @return err
 */
func (L *LRCTaskActuator) ExecTask(msg message.TaskDescription, opts Options) (err error) {
	L.msg = msg

	// @TODO 初始化LRC句柄
	err = L.initLRCHandler()
	if err != nil {
		return
	}

	// @TODO 下载分片
	ctx, cancel := context.WithTimeout(context.Background(), time.Now().Sub(opts.Expired))
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

	return
}
