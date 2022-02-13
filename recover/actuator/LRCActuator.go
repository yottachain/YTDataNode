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
	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58/base58"
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
	needIndexes []int16
	needDownloadIndexes []int16
	needIndexMap  map[int16] struct{}
	isInitHand	bool
}

/**
 * @Description: 初始化LRC
 * @receiver L
 * @return error
 */
func (L *LRCTaskActuator) initLRCHandler(stage RecoverStage) error {
	if !L.isInitHand {
		L.isInitHand = true
	}else {
		return nil
	}

	l := &lrc.Shardsinfo{}

	l.OriginalCount = uint16(len(L.msg.Hashs) - int(L.msg.ParityShardCount))
	l.RecoverNum = 13
	l.Lostindex = uint16(L.msg.RecoverId)
	l.GetRCHandle(l)
	L.lrcHandler = l

	log.Printf("[recover] task=%d stage=%d lost index %d\n", binary.BigEndian.Uint64(L.msg.Id[:8]),
		stage, L.msg.RecoverId)

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

	//_ = L.lrcHandler.SetHandleParam(L.lrcHandler.Handle, uint8(L.msg.RecoverId), uint8(L.opts.Stage-1))

	//@TODO 当没有已经缓存的分片时从LRC获取需要分片列表
	needList, _ := L.lrcHandler.GetNeededShardList(L.lrcHandler.Handle)

	//重置一下
	L.needIndexes = nil
	L.needIndexMap = nil
	indexMap := make(map[int16]struct{})

	for curr := needList.Front(); curr != nil; curr = curr.Next() {
		if index, ok := curr.Value.(int16); ok {
			indexMap[index] = struct{}{}
			L.needIndexes = append(L.needIndexes, index)
		} else {
			return nil, fmt.Errorf("get need shard list fail")
		}
	}

	L.needIndexMap = indexMap

	fmt.Printf("任务 %d 阶段 %d 需要的分片数%d indexes:%v\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, len(L.needIndexes), L.needIndexes)

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

	L.needDownloadIndexes = nil
	for key := range indexMap {
		L.needDownloadIndexes = append(L.needDownloadIndexes, key)
	}

	fmt.Printf("任务%d 阶段%d 需要下载的分片数%d indexes:%v\n",
		binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, len(L.needDownloadIndexes), L.needDownloadIndexes)

	return L.needDownloadIndexes, nil
}

/**
 * @Description: 添加下载任务
 * @receiver L
 * @params duration 每个任务最大等待时间
 * @param indexes 下载任务分片索引
 */
func (L *LRCTaskActuator) addDownloadTask(duration time.Duration, indexes ...int16) (*sync.WaitGroup, error) {
	//log.Println("[recover_debugtime] addDownloadTask start taskid=",
	//	binary.BigEndian.Uint64(L.msg.Id[:8]))

	wg := &sync.WaitGroup{}

	// @TODO 循环添加下载任务
	for _, shardIndex := range indexes {
		addrInfo := L.msg.Locations[shardIndex]
		hash := L.msg.Hashs[shardIndex]

		//  下载分片计数加一
		statistics.DefaultRebuildCount.IncShardForRbd()
		//log.Println("[recover_debugtime] addDownloadTask_addtask start taskid=",
		//	binary.BigEndian.Uint64(L.msg.Id[:8]))
		d, err := L.downloader.AddTask(addrInfo.NodeId, addrInfo.Addrs, hash)
		if err != nil {
			return nil, fmt.Errorf("add download task fail %s", err.Error())
		}

		//log.Println("[recover_debugtime]  E2_1  end addDownloadTask_addTask taskid=",
		//	binary.BigEndian.Uint64(L.msg.Id[:8]),"addrinfo=",addrInfo,"hash=",base58.Encode(hash))

		// @TODO 异步等待下载任务执行完成
		go func(key []byte, d shardDownloader.DownloaderWait, index int16, addrInfo *message.P2PLocation) {
			wg.Add(1)
			defer wg.Done()
			//log.Println("[recover_debugtime] start download goroutine in addDownloadTask  taskid=",
			//	binary.BigEndian.Uint64(L.msg.Id[:8]),"addrinfo=",addrInfo,"hash=",base58.Encode(key))
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			//log.Println("[recover_debugtime]  start addDownloadTask get_shard in download goroutine taskid=",
			//	binary.BigEndian.Uint64(L.msg.Id[:8]),"addrinfo=",addrInfo,"hash=",base58.Encode(key))
			shard, err := d.Get(ctx)
			if err != nil {
				log.Println("[recover_debugtime]  E2_2 end addDownloadTask get_shard taskid=",
					binary.BigEndian.Uint64(L.msg.Id[:8]),"addrinfo=",addrInfo,"hash=",base58.Encode(key),"error:",err)
			}

			// @TODO 如果因为分片不存在导致错误，直接中断
			//if err != nil && strings.Contains(err.Error(), "Get data Slice fail") {
			//	brk = true
			//}

			if err != nil && strings.Contains(err.Error(), "Get data Slice fail"){
				log.Printf("[recover_debugtime] download error in addDownloadTask " +
					"taskid=%d addrinfo=%s source hash=%s err=%s\n",
					binary.BigEndian.Uint64(L.msg.Id[:8]), addrInfo, base58.Encode(key), err.Error())
				return
			}

			dhash := md5.Sum(shard)
			L.shards.Set(hex.EncodeToString(key), &Shard{
				Index: index,
				Data:  shard,
				Check: bytes.Equal(key, dhash[:]),
			})

			////test
			//if shard != nil {
			//	log.Println("[recover_debugtime] end download goroutine in addDownloadTask  taskid=",
			//		binary.BigEndian.Uint64(L.msg.Id[:8]), "addrinfo=", addrInfo,
			//		"source hash=", base58.Encode(key), "download hash=", base58.Encode(dhash[:]),
			//		"hashEqual=", bytes.Equal(key, dhash[:]))
			//}else {
			//	log.Println("[recover_debugtime] end download goroutine in addDownloadTask  taskid=",
			//		binary.BigEndian.Uint64(L.msg.Id[:8]), "addrinfo=", addrInfo,
			//		"source hash=", base58.Encode(key))
			//}
		}(hash, d, shardIndex, addrInfo)
	}

	//log.Println("[recover_debugtime] addDownloadTask end taskid=",
	//	binary.BigEndian.Uint64(L.msg.Id[:8]))

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
	if L.isTimeOut() {
		return fmt.Errorf("lrc task time out")
	}
	if errCount > 5 {
		return fmt.Errorf("download times too many, try times %d", errCount)
	}
	errCount++

	//log.Println("[recover_debugtime] E0  taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]),"errcount:",errCount)

	select {
	case <-ctx.Done():
		return fmt.Errorf("download loop time out")
	default:
		if L.needDownloadIndexes == nil {
			_, err := L.getNeedShardList()
			if err != nil {
				log.Println("[recover_debugtime] E1 getNeedShardList  taskid=",
					binary.BigEndian.Uint64(L.msg.Id[:8]), "err:", err.Error(), "errcount:",errCount)
			}
		}

		fmt.Printf("任务:%d 阶段:%d 需要下载的分片数:%d indexes:%v 尝试:%d\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage,
			len(L.needDownloadIndexes), L.needDownloadIndexes, errCount)

		downloadTask, err := L.addDownloadTask(time.Minute*1, L.needDownloadIndexes...)
		//log.Println("[recover_debugtime] E2 addDownloadTask taskid=",
		//	binary.BigEndian.Uint64(L.msg.Id[:8]), "errcount:",errCount)
		if err != nil {
			return err
		}
		downloadTask.Wait()
		//log.Println("[recover_debugtime] E3 Wait taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]),
		//	"errcount:",errCount)

		if L.shards.Len() < len(L.needIndexes) {
			goto start
		}
		if ok := L.checkNeedShardsExist(); !ok {
			log.Println("[recover_debugtime] E4 checkNeedShardsExist taskid=",
				binary.BigEndian.Uint64(L.msg.Id[:8]), "errcount:", errCount)
			// @TODO 如果检查分片不足跳回开头继续下载
			goto start
		}
		//log.Println("[recover_debugtime] E5 addDownloadTask taskid=",
		//	binary.BigEndian.Uint64(L.msg.Id[:8]),"errcount:",errCount)
	}

	//log.Println("[recover_debugtime] E6 succ taskid=",
	//	binary.BigEndian.Uint64(L.msg.Id[:8]),"errcount:", errCount)

	return nil
}

/**
 * @Description: 预判重建能否成功
 * @receiver L
 * @return ok
 */
func (L *LRCTaskActuator) preJudge() (ok bool) {
	if L.isTimeOut() {
		return false
	}

	indexes, err := L.getNeedShardList()
	if err != nil {
		fmt.Printf("任务 %d 阶段 %d getNeedShardList err %s\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, err.Error())
		return false
	}

	var onLineShardIndexes = make([]int16, 0)
	var offLineShardIndexes = make([]int16, 0)
	for _, index := range indexes {
		if ok := activeNodeList.HasNodeid(L.msg.Locations[index].NodeId); ok {
			onLineShardIndexes = append(onLineShardIndexes, index)
		} else {
			offLineShardIndexes = append(offLineShardIndexes, index)
			// 如果是行列校验，所需分片必须都在线
			if L.opts.Stage < 3 {
				return false
			}
		}
	}

	if len(offLineShardIndexes) != 0 {
		fmt.Printf("任务 %d 阶段 %d 离线节点数%d offLine Indexes:%v\n",
			binary.BigEndian.Uint64(L.msg.Id[:8]), L.opts.Stage, len(offLineShardIndexes), offLineShardIndexes)
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
	if L.isTimeOut() {
		return nil, fmt.Errorf("task lrc backup timeout")
	}

	if L.msg.BackupLocation == nil {
		return nil, fmt.Errorf("no backup")
	}

	log.Println("[recover_debugtime] B0  backupTask taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]))
	if !activeNodeList.HasNodeid(L.msg.BackupLocation.NodeId) {
		return nil, fmt.Errorf("backup is offline, backup nodeid is %s", L.msg.BackupLocation.NodeId)
	}
	log.Println("[recover_debugtime] B1  HasNodeid taskid=", binary.BigEndian.Uint64(L.msg.Id[:8]))

	for i := 0; i < 5; i++ {
		dw, err := L.downloader.AddTask(L.msg.BackupLocation.NodeId,
						L.msg.BackupLocation.Addrs, L.msg.Hashs[L.msg.RecoverId])
		if err != nil {
			return nil, err
		}
		log.Println("[recover_debugtime] B2  addtask taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]),"i=",i,"hashid=",
			base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		data, err := dw.Get(ctx)
		log.Println("[recover_debugtime] B3  getdata taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]),"i=", i, "hashid=",
			base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
		if err != nil {
			continue
		}
		log.Println("[recover_debugtime] B4 succ recover taskid=",
			binary.BigEndian.Uint64(L.msg.Id[:8]),"i=",i,"hashid=",
			base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
		log.Printf("[recover] %s 从备份恢复成功 %d\n", hex.EncodeToString(L.msg.Hashs[L.msg.RecoverId]), i)
		return data, nil
	}

	log.Println("[recover_debugtime] B5  getdata fail taskid=",
		binary.BigEndian.Uint64(L.msg.Id[:8]),"hashid=",
		base58.Encode(L.msg.Hashs[L.msg.RecoverId]))
	return nil, fmt.Errorf("download backup fail, backup nodeid is %s", L.msg.BackupLocation.NodeId)
}

/**
 * @Description: 恢复数据
 * @receiver L
 * @return []byte
 * @return error
 */
func (L *LRCTaskActuator) recoverShard() ([]byte, error) {
	if L.isTimeOut() {
		return nil, fmt.Errorf("task lrc recouver shard time out")
	}

	//test -------------
	//for _, v := range L.shards.GetMap() {
	//	dhash := md5.Sum(v.Data)
	//	if !bytes.Equal(dhash[:], L.msg.Hashs[v.Index]) {
	//		fmt.Printf("recover, task=%d, hash inconsistent source hash %s, download hash %s\n",
	//			binary.BigEndian.Uint64(L.msg.Id[:8]), base58.Encode( L.msg.Hashs[v.Index]), base58.Encode(dhash[:]))
	//	}else{
	//		fmt.Printf("recover, task=%d, source hash %s, download hash %s\n",
	//			binary.BigEndian.Uint64(L.msg.Id[:8]), base58.Encode( L.msg.Hashs[v.Index]), base58.Encode(dhash[:]))
	//	}
	//}

	//_ = L.lrcHandler.SetHandleParam(L.lrcHandler.Handle, uint8(L.msg.RecoverId), uint8(L.opts.Stage))

	sIndexes := make([]int16, 0)
	Checks := make([]bool, 0)
	useIndexMap := make(map[int16]struct{})

	for _, v := range L.shards.GetMap() {
		if _, ok := L.needIndexMap[v.Index]; !ok {
			continue
		}
		sIndexes = append(sIndexes, v.Index)
		useIndexMap[v.Index] = struct{}{}

		Checks = append(Checks, v.Check)

		_, stage, err := L.lrcHandler.GetHandleParam(L.lrcHandler.Handle)
		if err != nil {
			fmt.Printf("recover Get Handle Param error %s\n", err.Error())
		}

		status, err := L.lrcHandler.AddShardData(L.lrcHandler.Handle, v.Data)
		if err != nil {
			fmt.Printf("recover add shard data error %s\n", err.Error())
		}

		fmt.Println("任务", binary.BigEndian.Uint64(L.msg.Id[:8]), "add shard ", "task stage=", L.opts.Stage,
			"lrc stage=", stage, "index", v.Index, "status", status, )

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
				"添加分片失败", "index",v.Index, "status", status,
				" 分片数据hash", base58.Encode(hash[:]), "err:", err)
		}
	}

	buf := bytes.NewBuffer([]byte{})
	for _, v := range L.shards.GetMap() {
		if _, ok := useIndexMap[v.Index]; !ok {
			continue
		}

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
	msgID = L.msg.Id
	recoverHash = L.msg.Hashs[L.msg.RecoverId]

	//每次重置一下这两个值
	L.needIndexes = nil
	L.needDownloadIndexes = nil
	L.needIndexMap = nil

	if err != nil {
		log.Println("[recover_debugtime] exectask error:", err.Error(), "taskid=",
			binary.BigEndian.Uint64(msgID[:8]))
		return
	}

	log.Println("[recover_debugtime] A  ExecTask start taskid=",
		binary.BigEndian.Uint64(msgID[:8]), "stage:", opts.Stage)

	// @TODO 如果是备份恢复阶段，直接执行备份恢复
	if L.opts.Stage == 0 {
		data, err = L.backupTask()
		log.Println("[recover_debugtime] B end taskid=",
			binary.BigEndian.Uint64(msgID[:8]))
		if err != nil {
			log.Println("[recover] backupTask error:", err, "taskid=",
				binary.BigEndian.Uint64(msgID[:8]))
		}else{
			statistics.DefaultRebuildCount.IncSuccRbd()
			log.Printf("[recover] backupTask success, shard hash is %s\n",
				base58.Encode(L.msg.Hashs[L.msg.RecoverId]),"taskid:",string(msgID[:8]))
		}
		return
	}

	// @TODO 初始化LRC句柄
	err = L.initLRCHandler(opts.Stage)
	//log.Println("[recover_debugtime] C taskid=", binary.BigEndian.Uint64(msgID[:8]))
	if err != nil {
		log.Println("[recover] initLRCHandler error:",err.Error())
		return
	}

	// @TODO 预判
	if ok := L.preJudge(); !ok {
		err = fmt.Errorf("预判失败 阶段 %d 任务 %d", L.opts.Stage, binary.BigEndian.Uint64(msgID[:8]))
		log.Println("[recover] preJudge error:", err.Error())
		return
	}
	//log.Println("[recover_debugtime] D preJudge taskid=", binary.BigEndian.Uint64(msgID[:8]))

	// 成功预判计数加一
	statistics.DefaultRebuildCount.IncPassJudge()

	// @TODO 下载分片
	//ctx, cancel := context.WithDeadline(context.Background(), opts.Expired)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opts.Expired)*time.Second)
	defer cancel()
	err = L.downloadLoop(ctx)
	//log.Println("[recover_debugtime] E downloadloop taskid=", binary.BigEndian.Uint64(msgID[:8]))
	if err != nil {
		log.Printf("[recover] task=%d stage=%d download loop err:%s\n",
			binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, err.Error())
		return
	}

	// @TODO LRC恢复
	recoverData, err := L.recoverShard()
	if err != nil {
		log.Printf("[recover] task=%d stage=%d recover Shard err:%s\n",
			binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, err.Error())
		return
	}

	//log.Println("[recover_debugtime] F end taskid=", binary.BigEndian.Uint64(msgID[:8]))
	// @TODO 验证数据
	if err = L.verifyLRCRecoveredData(recoverData); err != nil {
		log.Printf("[recover] task=%d stage=%d verify Recovered Data err:%s\n",
			binary.BigEndian.Uint64(msgID[:8]), L.opts.Stage, err.Error())
		return
	}

	//log.Println("[recover_debugtime] G end taskid=", binary.BigEndian.Uint64(msgID[:8]))
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
	if int32(time.Now().Sub(L.opts.STime).Seconds()) > L.opts.Expired {
		return true
	}

	return false
}

func New(downloader shardDownloader.ShardDownloader) *LRCTaskActuator {
	return &LRCTaskActuator{
		downloader: downloader,
		lrcHandler: nil,
		msg:        message.TaskDescription{},
	}
}
