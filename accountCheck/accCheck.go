package accountCheck

import (
	"bytes"
	"encoding/binary"
	"fmt"
	log "github.com/yottachain/YTDataNode/logger"
	sc "github.com/yottachain/YTDataNode/slicecompare"
	sni "github.com/yottachain/YTDataNode/storageNodeInterface"
	Ytfs "github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/common"
	"time"
)

const DnAccInfoPrefix = "keyDnAccInfoPrefix_"

const (
	Checking  = iota // 对账位完成
	CheckSuc         // 对账成功
	CheckFail        // 对账失败
)

type accCheckStatistics struct {
	checkSum uint32 //校验和
	xor      uint32 //异或值
	count    uint32 //分片数量
}

type AccCheckInfo struct {
	groups []accCheckStatistics //所有分组
	totals accCheckStatistics   //所有分组的统计
	m      uint32               //hash的低m位
	status uint
}

func TimerRun(ytfs *Ytfs.YTFS, cDb *sni.CompDB) {
	for {
		//每次对账间隔一定时间，应该把上次对账成功的时间记录在db里，
		//要不然间隔时间之内重启了，对账开始就不是间隔指定的时间了
		<-time.After(time.Minute * 14400)

		//send account checking req to sn, response m
		maxSeq, err := sc.GetSeqFromDb(cDb, sc.FinishKey)
		if err != nil {
			log.Printf("account checking GetSeqFromDb error %s\n", err)
			continue
		}
		//查询是否已经对账过了
		accInfo, err := GetStatisticsToDb(cDb, maxSeq)
		if err == nil && accInfo != nil {
			log.Printf("account checking seq:%d checked!\n", maxSeq)
			continue
		}

		//send maxSeq to sn, resp m
		accInfo = New(16)

		err = ytfs.YtfsDB().TravelDbForAccountCheck(CalcGroupInfo, accInfo, maxSeq)
		if err != nil {
			log.Printf("account checking error %s\n", err)
		}

		//计算总数，和sn的数据对比
		_ := TotalStatistics(accInfo)

		//结果保存到db
		err = PutStatisticsToDb(cDb, accInfo, maxSeq)
		if err != nil {
			log.Printf("account checking error %s\n", err)
		}
		//查询sn的计算结果 然后开始对比

		//对账成功
		accInfo.status = CheckSuc
		//结果保存到db
		err = PutStatisticsToDb(cDb, accInfo, maxSeq)
		if err != nil {
			log.Printf("account checking error %s\n", err)
		}

		//对账失败
		accInfo.status = CheckFail
		//结果保存到db
		err = PutStatisticsToDb(cDb, accInfo, maxSeq)
		if err != nil {
			log.Printf("account checking error %s\n", err)
		}
	}
}

func New(m uint32) *AccCheckInfo {
	acInfo := new(AccCheckInfo)
	count := 1 << m
	acInfo.groups = make([]accCheckStatistics, count, count)
	acInfo.m = m
	acInfo.status = Checking

	return acInfo
}

func CalcGroupInfo(
	hash common.Hash,
	acInfoPtr interface{}) (err error) {
	var acInfo AccCheckInfo

	if value, ok := acInfoPtr.(AccCheckInfo); !ok {
		err = fmt.Errorf("acInfoPtr illegal")
		return
	} else {
		acInfo = value
	}

	m := acInfo.m

	if m > 32 || m == 0 {
		err = fmt.Errorf("m value illegal")
		return
	}

	var byteCount uint32
	if m%8 > 0 {
		byteCount = m/8 + 1
	} else {
		byteCount = m / 8
	}

	l := len(hash)
	if l < 16 {
		err = fmt.Errorf("hash len illegal")
		return
	}
	startPos := uint32(l) - byteCount
	maxValue := binary.LittleEndian.Uint32(hash[startPos:])
	group := maxValue & (1<<m - 1)
	var hashArray [4]uint32
	hashArray[0] = binary.LittleEndian.Uint32(hash[0:4])
	hashArray[1] = binary.LittleEndian.Uint32(hash[4:8])
	hashArray[2] = binary.LittleEndian.Uint32(hash[8:12])
	hashArray[3] = binary.LittleEndian.Uint32(hash[12:])

	sum := hashArray[0] + hashArray[1] + hashArray[2] + hashArray[3]
	acInfo.groups[group].checkSum += sum

	xor := hashArray[0] ^ hashArray[1] ^ hashArray[2] ^ hashArray[3]
	acInfo.groups[group].xor ^= xor

	acInfo.groups[group].count += 1

	return
}

func TotalStatistics(acInfo *AccCheckInfo) (acs accCheckStatistics) {
	for _, group := range acInfo.groups {
		acs.checkSum += group.checkSum
		acs.xor ^= group.xor
		acs.count += group.count
	}

	acInfo.totals = acs

	return
}

func PutStatisticsToDb(cDb *sni.CompDB, acInfo *AccCheckInfo, maxSeq uint64) error {
	value, err := EncodeToByte(acInfo)
	if err != nil {
		log.Printf("account checking EncodeToByte error %s\n", err)
		return err
	}
	keyStr := fmt.Sprintf("%s%d", DnAccInfoPrefix, maxSeq)
	err = cDb.Db.Put(cDb.Wo, []byte(keyStr), value)
	return err
}

func GetStatisticsToDb(cDb *sni.CompDB, maxSeq uint64) (*AccCheckInfo, error) {
	keyStr := fmt.Sprintf("%s%d", DnAccInfoPrefix, maxSeq)
	data, err := cDb.Db.Get(cDb.Ro, []byte(keyStr))
	if err != nil {
		return nil, err
	}

	if data.Data() == nil {
		return nil, nil
	}
	return DecodeToAccInfo(data.Data())
}

func EncodeToByte(accInfo *AccCheckInfo) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, accInfo); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeToAccInfo(b []byte) (*AccCheckInfo, error) {
	buf := bytes.NewBuffer(b)

	obj := &AccCheckInfo{}

	if err := binary.Read(buf, binary.LittleEndian, obj); err != nil {
		return nil, err
	}

	return obj, nil
}
