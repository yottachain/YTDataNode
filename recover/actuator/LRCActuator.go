/**
 * @Description: LRC执行器的实现
 * @Author: LuYa 2021-04-28
 */
package actuator

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTDataNode/activeNodeList"
	"github.com/yottachain/YTDataNode/config"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTDataNode/recover/shardDownloader"
	"github.com/yottachain/YTDataNode/statistics"
	lrc "github.com/yottachain/YTLRC"
)

const dlTryCount = 6

type Shard struct {
	Index int16
	Data  []byte
	Check bool
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
	s.mutex = sync.Mutex{}
	return &s
}

/**
 * @Description: LRC执行器
 */
type LRCTaskActuator struct {
	downloader           shardDownloader.ShardDownloader // 下载器
	lrcHandler           *lrc.Shardsinfo                 // lrc句柄
	msg                  message.TaskDescription         // 重建消息
	shards               *shardsMap                      // 重建所需分片
	opts                 Options
	needIndexes          []int16
	needDownloadIndexes  []int16
	needDownloadIndexMap map[int16]struct{}
	downloadSuccs        int
	isInitHand           bool
	lck                  *sync.Mutex //来自引擎的全局锁
	pfStat               *statistics.PerformanceStat
	hashAddrMap          map[string]*[2]int //下载分片hash对应的两个地址使用计数
}

/**
 * @Description: 初始化LRC
 * @receiver L
 * @return error
 */
func (L *LRCTaskActuator) initLRCHandler(stage RecoverStage) error {
	if L.isInitHand {
		return nil
	}

	l := &lrc.Shardsinfo{}

	l.OriginalCount = uint16(len(L.msg.Hashs) - int(L.msg.ParityShardCount))
	l.RecoverNum = config.GlobalParityShardNum
	l.Lostindex = uint16(L.msg.RecoverId)
	l.ShardSize = uint32(config.GlobalShardSize * 1024)

	L.lck.Lock()
	handle := l.GetRCHandle(l)
	L.lck.Unlock()

	if handle == nil {
		return fmt.Errorf("lrc get handle nil")
	}
	L.lrcHandler = l

	log.Printf("[recover] task=%d stage=%d lost index %d shard hash:%s\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]),
		stage, L.msg.RecoverId, base58.Encode(L.msg.Hashs[L.msg.RecoverId]))

	L.shards = shardsMap{}.Init()
	L.isInitHand = true

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

	//_ = L.lrcHandler.SetHandleParam(L.lrcHandler.Handle, uint8(L.msg.RecoverId), uint8(L.opts.Stage-1))

	//@TODO 当没有已经缓存的分片时从LRC获取需要分片列表
	needList, _ := L.lrcHandler.GetNeededShardList(L.lrcHandler.Handle)

	_, stage, _ := L.lrcHandler.GetHandleParam(L.lrcHandler.Handle)

	//重置一下
	L.needIndexes = nil
	L.needDownloadIndexMap = make(map[int16]struct{})
	L.downloadSuccs = 0

	indexMap := make(map[int16]struct{})

	for curr := needList.Front(); curr != nil; curr = curr.Next() {
		if index, ok := curr.Value.(int16); ok {
			indexMap[index] = struct{}{}
			L.needIndexes = append(L.needIndexes, index)
		} else {
			return nil, fmt.Errorf("get need shard list fail")
		}
	}

	//L.needDownloadIndexMap = indexMap

	log.Printf("[recover] 任务 %d 阶段 %d real stage %d 需要的分片数%d  indexes:%v\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, stage, len(L.needIndexes), L.needIndexes)

	// @TODO 如果已经有部分分片下载成功了则只检查未下载成功分片
	if L.shards.Len() > 0 {
		for _, shard := range L.shards.GetMap() {
			if _, ok := indexMap[shard.Index]; ok {
				//有数据就不需要下载了
				if shard.Data != nil {
					delete(indexMap, shard.Index)
				}
			}
		}
	}

	//L.needDownloadIndexes = nil
	for key := range indexMap {
		L.needDownloadIndexes = append(L.needDownloadIndexes, key)
		L.needDownloadIndexMap[key] = struct{}{}
	}

	log.Printf("[recover] 任务 %d 阶段 %d real stage %d 需要下载的分片数%d indexes:%v\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, stage, len(L.needDownloadIndexes), L.needDownloadIndexes)

	return L.needDownloadIndexes, nil
}

/**
 * @Description: 添加下载任务
 * @receiver L
 * @params duration 每个任务最大等待时间
 * @param indexes 下载任务分片索引
 */
func (L *LRCTaskActuator) addDownloadTask(duration time.Duration, indexes ...int16) []byte {
	//log.Println("[recover_debugtime] addDownloadTask start taskid=",
	//	base58.Encode(L.msg.Id[:]))

	wg := &sync.WaitGroup{}
	var recoverData []byte

	// @TODO 循环添加下载任务
	for _, shardIndex := range indexes {
		addrInfo := L.msg.Locations[shardIndex]
		hash := L.msg.Hashs[shardIndex]
		strHash := base58.Encode(hash)

		//下载分片计数加一
		statistics.DefaultRebuildCount.IncShardForRbd()

		log.Println("[recover_debugtime] E2_1 addDownloadTask_addtask start taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
			"hash=", base58.Encode(hash))
		if nil == L.pfStat.DlShards[shardIndex] {
			L.pfStat.DlShards[shardIndex] = &statistics.ShardPerformanceStat{
				Hash:  strHash,
				DlSuc: false}
		}

		//index 不应该>=164
		L.pfStat.DlShards[shardIndex].DlStartTime = time.Now()
		L.pfStat.DlShards[shardIndex].DlTimes++

		var d shardDownloader.DownloaderWait

		if peerNode := activeNodeList.GetActiveNodeData(addrInfo.NodeId); peerNode != nil {
			if L.hashAddrMap[strHash][0]/2 > 0 && L.hashAddrMap[strHash][1] < L.hashAddrMap[strHash][0] {
				if peerNode2 := activeNodeList.GetActiveNodeData(addrInfo.NodeId2); peerNode2 != nil {
					d, _ = L.downloader.AddTask(addrInfo.NodeId2, peerNode2.IP, hash,
						binary.BigEndian.Uint64(L.msg.Id[:8]), int(L.opts.Stage))
					log.Printf("[recover_debugtime] E2_1 addDownloadTask_addTask taskid=%d "+
						"index %d use node(2) nodeid %s addrinfo %v\n", binary.BigEndian.Uint64(L.msg.Id[:8]),
						shardIndex, addrInfo.NodeId2, peerNode2.IP)
					L.hashAddrMap[strHash][1]++
				} else {
					d, _ = L.downloader.AddTask(addrInfo.NodeId, peerNode.IP, hash,
						binary.BigEndian.Uint64(L.msg.Id[:8]), int(L.opts.Stage))
					log.Printf("[recover_debugtime] E2_1 addDownloadTask_addTask taskid=%d "+
						"index %d use node(1) nodeid %s addrinfo %v\n", binary.BigEndian.Uint64(L.msg.Id[:8]),
						shardIndex, addrInfo.NodeId, peerNode.IP)
					L.hashAddrMap[strHash][0]++
				}
			} else {
				d, _ = L.downloader.AddTask(addrInfo.NodeId, peerNode.IP, hash,
					binary.BigEndian.Uint64(L.msg.Id[:8]), int(L.opts.Stage))
				log.Printf("[recover_debugtime] E2_1 addDownloadTask_addTask taskid=%d "+
					"index %d use node(1) nodeid %s addrinfo %v\n", binary.BigEndian.Uint64(L.msg.Id[:8]),
					shardIndex, addrInfo.NodeId, peerNode.IP)
				L.hashAddrMap[strHash][0]++
			}
		} else if peerNode2 := activeNodeList.GetActiveNodeData(addrInfo.NodeId); peerNode2 != nil {
			d, _ = L.downloader.AddTask(addrInfo.NodeId2, peerNode2.IP, hash,
				binary.BigEndian.Uint64(L.msg.Id[:8]), int(L.opts.Stage))
			log.Printf("[recover_debugtime] E2_1 addDownloadTask_addTask taskid=%d "+
				"index %d use node(2) nodeid %s addrinfo %v\n", binary.BigEndian.Uint64(L.msg.Id[:8]),
				shardIndex, addrInfo.NodeId2, peerNode2.IP)
			L.hashAddrMap[strHash][1]++
		} else {
			log.Println("[recover_debugtime] E2_1 addDownloadTask_addtask fail taskid=",
				binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
				"hash=", base58.Encode(hash), "err=all node offline")
			continue
		}

		log.Println("[recover_debugtime]  E2_1 addDownloadTask_addTask end taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
			"hash=", base58.Encode(hash))

		// @TODO 异步等待下载任务执行完成
		wg.Add(1)
		go func(key []byte, dl shardDownloader.DownloaderWait, index int16, addrInfo *message.P2PLocation) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			log.Println("[recover_debugtime]  E2_2 start addDownloadTask get_shard in download goroutine taskid=",
				binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
				"hash=", base58.Encode(key), "index=", index)
			shard, err := dl.Get(ctx)
			if err != nil {
				log.Println("[recover_debugtime]  E2_2 end addDownloadTask get_shard taskid=",
					binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
					"hash=", base58.Encode(key), "error:", err)
			}

			// @TODO 如果因为分片不存在导致错误，直接中断
			if err != nil && strings.Contains(err.Error(), "Get data Slice fail") {
				log.Printf("[recover_debugtime] E2_2 download error in addDownloadTask "+
					"taskid=%d addrinfo=%s source hash=%s err=%s\n",
					binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage, addrInfo, base58.Encode(key), err.Error())
				return
			}

			dhash := md5.Sum(shard)
			L.shards.Set(hex.EncodeToString(key), &Shard{
				Index: index,
				Data:  shard,
				Check: bytes.Equal(key, dhash[:]),
			})

			if shard != nil {
				L.pfStat.DlShards[index].DlSuc = true
				L.pfStat.DlShards[index].DlUseTime = time.Now().Sub(L.pfStat.DlShards[index].DlStartTime).Milliseconds()

				L.lck.Lock()
				//删除已经下载成功的 下一轮就下载没有下载成功的
				delete(L.needDownloadIndexMap, index)
				L.downloadSuccs++

				if recoverData == nil {
					recoverData, err = L.recoverShard(shard, index)
				}
				L.lck.Unlock()
			}

			////test
			if shard != nil {
				log.Println("[recover_debugtime] E2_2 end download goroutine in addDownloadTask  taskid=",
					base58.Encode(L.msg.Id[:]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
					"source hash=", base58.Encode(key), "download hash=", base58.Encode(dhash[:]),
					"hashEqual=", bytes.Equal(key, dhash[:]))
			} else {
				log.Println("[recover_debugtime] E2_2 end download goroutine in addDownloadTask  taskid=",
					base58.Encode(L.msg.Id[:]), "stage=", L.opts.Stage, "addrinfo=", addrInfo,
					"source hash=", base58.Encode(key), "get shard fail")
			}
		}(hash, d, shardIndex, addrInfo)
	}

	//log.Println("[recover_debugtime] addDownloadTask end taskid=",
	//	base58.Encode(L.msg.Id[:]))

	wg.Wait()

	return recoverData
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
func (L *LRCTaskActuator) downloadLoop(ctx context.Context) ([]byte, error) {
	var errCount uint64
start:
	if L.isTimeOut() {
		return nil, fmt.Errorf("lrc task time out")
	}
	errCount++
	if errCount > dlTryCount {
		//Although download shards no enough, also into rebuild process
		return nil, fmt.Errorf("try download times too many")
	}

	//log.Println("[recover_debugtime] E0  taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]),"errcount:",errCount)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("download loop time out")
	default:
		if L.needDownloadIndexes == nil {
			_, err := L.getNeedShardList()
			if err != nil {
				log.Println("[recover_debugtime] E1 getNeedShardList  taskid=",
					binary.BigEndian.Uint64(L.msg.Id[:8]), "err:", err.Error(), "errcount:", errCount)
			}
		}

		var indexs []int16
		for key := range L.needDownloadIndexMap {
			indexs = append(indexs, key)
		}

		//test
		if errCount == 1 {
			log.Printf("任务:%d 阶段:%d first downloadLoop 需要下载的分片数:%d indexes:%v 尝试:%d\n",
				binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage,
				len(indexs), indexs, errCount)
		} else {
			log.Printf("任务:%d 阶段:%d downloadLoop 需要下载的分片数:%d indexes:%v 尝试:%d\n",
				binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage,
				len(indexs), indexs, errCount)
		}

		startTime := time.Now()
		L.downloadSuccs = 0 //init at before download
		recover := L.addDownloadTask(time.Minute*1, indexs...)
		useTime := time.Now().Sub(startTime).Milliseconds()
		log.Printf("任务:%d 阶段:%d 需要下载的分片数:%d 尝试:%d download_succs %d download_use_time %d ms\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage,
			len(indexs), errCount, L.downloadSuccs, useTime)

		if recover != nil {
			return recover, nil
		}

		//log.Println("[recover_debugtime] E3 Wait taskid=", base58.Encode(L.msg.Id[:]),
		//	"errcount:",errCount)

		if len(L.needDownloadIndexMap) > 0 {
			goto start
		}

		//if L.shards.Len() < len(L.needIndexes) {
		//	goto start
		//}
		//if ok := L.checkNeedShardsExist(); !ok {
		//	log.Println("[recover_debugtime] E4 checkNeedShardsExist taskid=",
		//		binary.BigEndian.Uint64(L.msg.Id[:8]), "errcount:", errCount)
		//	// @TODO 如果检查分片不足跳回开头继续下载
		//	goto start
		//}
		//log.Println("[recover_debugtime] E5 addDownloadTask taskid=",
		//	base58.Encode(L.msg.Id[:]),"errcount:",errCount)
	}

	//log.Println("[recover_debugtime] E6 succ taskid=",
	//	base58.Encode(L.msg.Id[:]),"errcount:", errCount)

	return nil, fmt.Errorf("shards no enough")
}

/**
 * @Description: 预判重建能否成功
 * @receiver L
 * @return ok
 */
func (L *LRCTaskActuator) preJudge() (ok bool) {
	if L.isTimeOut() {
		log.Printf("[recover] 任务 %d 阶段 %d lrc task timeout\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage)
		return false
	}

	startTime := time.Now()
	indexes, err := L.getNeedShardList()
	if err != nil {
		log.Printf("[recover] 任务 %d 阶段 %d getNeedShardList err %s\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, err.Error())
		return false
	}

	log.Printf("[recover] 任务 %d 阶段 %d get Need Shard List use time %d\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())

	startTime = time.Now()
	for _, index := range indexes {
		ok := activeNodeList.HasNodeid(L.msg.Locations[index].NodeId)
		ok1 := activeNodeList.HasNodeid(L.msg.Locations[index].NodeId2)
		if !ok && !ok1 {
			//onLineShardIndexes = append(onLineShardIndexes, index)

			log.Printf("[recover] 任务 %d 阶段 %d offline miner index %d (1) node_id %s (2) node_id %s",
				binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, index, L.msg.Locations[index].NodeId,
				L.msg.Locations[index].NodeId2)
			//不在线的矿机就不下载了, 两个地址都不在线
			delete(L.needDownloadIndexMap, index)
		}

		if ok {
			log.Printf("[recover] 任务 %d 阶段 %d online miner index %d (1) node_id %s",
				binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, index, L.msg.Locations[index].NodeId)
		}

		if ok1 {
			log.Printf("[recover] 任务 %d 阶段 %d online miner index %d (2) node_id %s",
				binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, index, L.msg.Locations[index].NodeId2)
		}
	}
	log.Printf("[recover] 任务 %d 阶段 %d online miner judge use time %d\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())

	return true
}

func (L *LRCTaskActuator) backupTask() ([]byte, error) {
	if L.isTimeOut() {
		return nil, fmt.Errorf("task lrc backup timeout")
	}

	if L.msg.BackupLocation == nil {
		return nil, fmt.Errorf("no backup")
	}

	log.Println("[recover_debugtime] B0  backupTask taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]))
	/*
		if !activeNodeList.HasNodeid(L.msg.BackupLocation.NodeId) {
			return nil, fmt.Errorf("backup is offline, backup nodeid is %s", L.msg.BackupLocation.NodeId)
		}*/
	var peerNode *activeNodeList.Data
	if peerNode = activeNodeList.GetActiveNodeData(L.msg.BackupLocation.NodeId); peerNode == nil {
		return nil, fmt.Errorf("backup is offline, backup nodeid is %s", L.msg.BackupLocation.NodeId)
	}
	log.Println("[recover_debugtime] B1  HasNodeid taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]))

	for i := 0; i < 5; i++ {
		dw, err := L.downloader.AddTask(L.msg.BackupLocation.NodeId,
			peerNode.IP, L.msg.Hashs[L.msg.RecoverId],
			binary.BigEndian.Uint64(L.msg.Id[:8]), int(L.opts.Stage))
		if err != nil {
			return nil, err
		}
		log.Println("[recover_debugtime] B2  addtask taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]), "i=", i, "hashid=",
			base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		data, err := dw.Get(ctx)
		log.Println("[recover_debugtime] B3  getdata taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]), "i=", i, "hashid=",
			base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
		if err != nil {
			continue
		}
		log.Println("[recover_debugtime] B4 succ recover taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]), "i=", i, "hashid=",
			base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
		log.Printf("[recover] %s 从备份恢复成功 %d\n", hex.EncodeToString(L.msg.Hashs[L.msg.RecoverId]), i)
		return data, nil
	}

	log.Println("[recover_debugtime] B5  getdata fail taskid=",
		binary.BigEndian.Uint64(L.msg.Id[:8]), "hashid=",
		base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
	return nil, fmt.Errorf("download backup fail, backup nodeid is %s", L.msg.BackupLocation.NodeId)
}

/**
 * @Description: 恢复数据
 * @receiver L
 * @return []byte
 * @return error
 */
func (L *LRCTaskActuator) recoverShard(shard []byte, index int16) ([]byte, error) {
	status, err := L.lrcHandler.AddShardData(L.lrcHandler.Handle, shard)
	if err != nil {
		fmt.Printf("recover add shard data error %s\n", err.Error())
		return nil, fmt.Errorf("recover add shard data error %s\n", err.Error())
	}

	if status > 0 {
		data, status := L.lrcHandler.GetRebuildData(L.lrcHandler)
		if data == nil {
			return nil, fmt.Errorf("recover data fail, status: %d", status)
		}
		return data, nil
	} else if status < 0 {
		hash := md5.Sum(shard)
		fmt.Println("task=", binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage,
			"添加分片失败", "index", index, "status", status,
			"分片数据hash", base58.Encode(hash[:]))
		err = fmt.Errorf("task=%d stage=%d 添加分片失败 index=%d status=%d 分片数据hash=%s\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, index, status,
			base58.Encode(hash[:]))
	} else {
		hash := md5.Sum(shard)
		fmt.Println("task=", binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage,
			"添加分片成功但是重建分片不足", "index", index, "status", status,
			"分片数据hash", base58.Encode(hash[:]))
		err = fmt.Errorf("task=%d stage=%d 添加分片成功但是重建分片不足 index=%d status=%d 分片数据hash=%s\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, index, status,
			base58.Encode(hash[:]))
	}

	return nil, err
}

func (L *LRCTaskActuator) recoverShard_bak() ([]byte, error) {
	if L.isTimeOut() {
		return nil, fmt.Errorf("task lrc recouver shard time out")
	}

	//_ = L.lrcHandler.SetHandleParam(L.lrcHandler.Handle, uint8(L.msg.RecoverId), uint8(L.opts.Stage))

	sIndexes := make([]int16, 0)
	Checks := make([]bool, 0)
	useIndexMap := make(map[int16]struct{})

	for _, v := range L.shards.GetMap() {
		if v.Data == nil {
			continue
		}
		sIndexes = append(sIndexes, v.Index)
		useIndexMap[v.Index] = struct{}{}

		Checks = append(Checks, v.Check)

		_, stage, err := L.lrcHandler.GetHandleParam(L.lrcHandler.Handle)
		if err != nil {
			fmt.Printf("recover Get Handle Param error %s\n", err.Error())
		}

		L.lck.Lock()
		status, err := L.lrcHandler.AddShardData(L.lrcHandler.Handle, v.Data)
		L.lck.Unlock()

		if err != nil {
			fmt.Printf("recover add shard data error %s\n", err.Error())
		}

		fmt.Println("任务", binary.BigEndian.Uint64(L.msg.Id[:8]), "add shard ", "task stage=", L.opts.Stage,
			"lrc stage=", stage, "index", v.Index, "status", status)

		if status > 0 {
			data, status := L.lrcHandler.GetRebuildData(L.lrcHandler)
			if data == nil {
				return nil, fmt.Errorf("recover data fail, status: %d", status)
			}
			fmt.Printf("recover, task=%d, stage=%d need indexes %v, used indexes %v, used checks %v\n",
				binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, L.needIndexes, sIndexes, Checks)
			return data, nil
		} else if status < 0 {
			hash := md5.Sum(v.Data)
			fmt.Println("task=", binary.BigEndian.Uint64(L.msg.Id[:8]), "stage=", L.opts.Stage,
				"添加分片失败", "index", v.Index, "status", status,
				"分片数据hash", base58.Encode(hash[:]), "err:", err)
		}
	}

	buf := bytes.NewBuffer([]byte{})
	for _, v := range L.shards.GetMap() {
		//if _, ok := useIndexMap[v.Index]; !ok {
		//	continue
		//}

		if v.Data != nil {
			hash := md5.Sum(v.Data)
			_, _ = fmt.Fprintln(
				buf,
				"恢复失败 已添加",
				fmt.Sprintf("index %d 分片hash %s 原hash %s 任务 %d 阶段 %d",
					v.Index,
					base58.Encode(hash[:]),
					base58.Encode(L.msg.Hashs[v.Index]),
					binary.BigEndian.Uint64(L.msg.Id[:8]),
					L.opts.Stage,
				))
		}
	}
	log.Println(buf.String())

	return nil, fmt.Errorf("任务 %d 阶段 %d 缺少分片",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage)
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
		fmt.Printf("verify recovered data len is %d\n", len(recoverData))
		return fmt.Errorf("recovered data hash verify fail task=%d recover hash:%s source hash:%s",
			binary.BigEndian.Uint64(L.msg.Id[:8]), base58.Encode(hash), base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
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
func (L *LRCTaskActuator) ExecTask(msgData []byte, opts Options) (data []byte,
	msgID []byte, recoverHash []byte, err error) {
	L.opts = opts
	err = L.parseMsgData(msgData)
	if err != nil {
		log.Println("[recover_debugtime] exectask parseMsgData error:", err.Error())
		return
	}

	msgID = L.msg.Id
	recoverHash = L.msg.Hashs[L.msg.RecoverId]

	//每次重置一下这两个值
	L.needIndexes = nil
	L.needDownloadIndexes = nil
	L.needDownloadIndexMap = nil

	//初始化一下
	for _, v := range L.msg.Hashs {
		if _, ok := L.hashAddrMap[base58.Encode(v)]; !ok {
			L.hashAddrMap[base58.Encode(v)] = new([2]int)
		}
	}

	log.Println("[recover_debugtime] A  ExecTask start taskid=",
		binary.BigEndian.Uint64(msgID[:8]), "stage:", opts.Stage)

	// @TODO 如果是备份恢复阶段，直接执行备份恢复
	if L.opts.Stage == 0 && !config.Gconfig.Lrc2BackUpOff {
		data, err = L.backupTask()
		//log.Println("[recover_debugtime] B end taskid=",
		//	binary.BigEndian.Uint64(msgID[:8]))
		if err != nil {
			log.Println("[recover] backupTask error:", err, "taskid=",
				binary.BigEndian.Uint64(msgID[:8]))
		} else {
			statistics.DefaultRebuildCount.IncSuccRbd()
			log.Printf("[recover] backupTask success, shard hash is %s\n",
				base58.Encode(L.msg.Hashs[L.msg.RecoverId]),
				"taskid=", binary.BigEndian.Uint64(msgID[:8]))
		}
		return
	}

	// @TODO 初始化LRC句柄
	startTime := time.Now()
	err = L.initLRCHandler(opts.Stage)
	//log.Println("[recover_debugtime] C taskid=", binary.BigEndian.Uint64(msgID[:8]))
	if err != nil {
		log.Println("[recover] initLRCHandler error:", err.Error())
		return
	}
	log.Printf("[recover] task=%d stage=%d init lrc use times %d ms",
		binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())

	// @TODO 预判
	startTime = time.Now()
	if ok := L.preJudge(); !ok {
		err = fmt.Errorf("预判失败 阶段 %d 任务 %d", L.opts.Stage, binary.BigEndian.Uint64(msgID[:8]))
		log.Println("[recover] preJudge error:", err.Error())
		return
	}
	log.Printf("[recover] task=%d stage=%d preJudge use times %d ms",
		binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())
	//log.Println("[recover_debugtime] D preJudge taskid=", binary.BigEndian.Uint64(msgID[:8]))

	// 成功预判计数加一
	statistics.DefaultRebuildCount.IncPassJudge()

	// @TODO 下载分片
	//ctx, cancel := context.WithDeadline(context.Background(), opts.Expired)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opts.Expired)*time.Second)
	defer cancel()

	startTime = time.Now()
	recoverData, err := L.downloadLoop(ctx)
	log.Printf("[recover] task=%d stage=%d download loop use times %d ms",
		binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())

	//log.Println("[recover_debugtime] E downloadloop taskid=", base58.Encode(msgID[:]))
	if err != nil {
		log.Printf("[recover] task=%d stage=%d download loop err:%s\n",
			binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, err.Error())
		return
	}

	//// @TODO LRC恢复
	//startTime = time.Now()
	//recoverData, err = L.recoverShard_bak()
	//log.Printf("[recover] task=%d stage=%d recover Shard use times %dms",
	//	binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())
	//
	//if err != nil {
	//	log.Printf("[recover] task=%d stage=%d recover Shard err:%s\n",
	//		binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, err.Error())
	//	return
	//}

	//log.Println("[recover_debugtime] F end taskid=", base58.Encode(msgID[:]))
	// @TODO 验证数据
	startTime = time.Now()
	if err = L.verifyLRCRecoveredData(recoverData); err != nil {
		log.Printf("[recover] task=%d stage=%d verify Recovered Data err:%s\n",
			binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, err.Error())
		return
	}
	log.Printf("[recover] task=%d stage=%d recover verify Shard use times %d ms",
		binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, time.Now().Sub(startTime).Milliseconds())

	//log.Println("[recover_debugtime] G end taskid=", base58.Encode(msgID[:]))
	data = recoverData
	// 成功重建计数加一
	statistics.DefaultRebuildCount.IncSuccRbd()
	return
}

func (L *LRCTaskActuator) Free() {
	if L.lrcHandler == nil {
		return
	}
	L.lrcHandler.FreeHandle()
	L.lrcHandler.FreeRecoverData()
}

func (L *LRCTaskActuator) isTimeOut() bool {
	//暂时不判断超时了
	//if int32(time.Now().Sub(L.opts.STime).Seconds()) > L.opts.Expired {
	//	return true
	//}

	return false
}

func (L *LRCTaskActuator) GetdownloadShards() *shardsMap {
	return L.shards
}

func (L *LRCTaskActuator) SetPfStat(pfstat *statistics.PerformanceStat) {
	L.pfStat = pfstat
}

func New(downloader shardDownloader.ShardDownloader) *LRCTaskActuator {
	return &LRCTaskActuator{
		downloader:  downloader,
		lrcHandler:  nil,
		msg:         message.TaskDescription{},
		lck:         &sync.Mutex{},
		hashAddrMap: make(map[string]*[2]int, 0),
	}
}
