package config

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "github.com/yottachain/YTDataNode/logger"
	"github.com/yottachain/YTDataNode/util"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"reflect"
	"time"
)

var update_url = "http://dnapi.yottachain.net/config/dnconfig.json"
var IsDev = 0

type UpdateHandler func(gc Gcfg)

type Gcfg struct {
	MaxToken              int    `json:"MaxToken"`
	MinToken              int    `json:"MinToken"`
	TTL                   int64  `json:"TTL"`
	Increase              int64  `json:"Increase"`
	RXIncreaseThreshold   int64  `json:"RXIncreaseThreshold"`
	TXIncreaseThreshold   int64  `json:"TXIncreaseThreshold"`
	Decrease              int64  `json:"Decrease"`
	RXDecreaseThreshold   int64  `json:"RXDecreaseThreshold"`
	TXDecreaseThreshold   int64  `json:"TXDecreaseThreshold"`
	TokenWait             int64  `json:"TokenWait"`
	TokenReturnWait       int64  `json:"TokenReturnWait"`
	Clean                 int    `json:"Clean"`
	ShardRbdConcurrent    uint16 `json:"ShardRbdConcurrent"`
	OutlineTimeRange      int    `json:"OutlineTimeRange"`
	MinVersion            int    `json:"MinVersion"`
	BanTime               int
	ElkReport             bool
	ElkReport2            bool
	DiskTimeout           int
	RXTestNum             int
	TXTestNum             int
	RandDownloadGroupSize int
	RXTestSleep           int
	TXTestSleep           int
	TestInterval		  int	//minute
	RXTestDuration		  int	//second
	TXTestDuration		  int	//second
	NodeListUpdateTime    int
	GcOpen                bool
	SliceCompareOpen      bool
	ActiveNodeUrl   	  string
	ActiveNodeUrlNew	  []string
	ActiveNodeTTL		  int		`json:"ActiveNodeTTL"`	//second
	ActiveNodeKey		  string
	VerifyReportMaxNum	  uint64	`json:"VerifyReportMaxNum"`
	SnApiServerUrl		  string
	RebuildMaxCc		  int
 }


func (g Gcfg) IsEqua(ng Gcfg) bool {
	return reflect.DeepEqual(&g, &ng)
}

type GConfig struct {
	Gcfg
	OnUpdate UpdateHandler
}

// Get 远程获取配置并更新
func (gc *GConfig) Get() error {
	gurl, ok := os.LookupEnv("gconfig_url")
	if ok {
		update_url = gurl
		log.Println("[gurl]", update_url)
	}

	request, err := http.NewRequest("GET", update_url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("%s", err)
	}

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_old := gc.Gcfg
	var _new Gcfg
	err = json.Unmarshal(resBuf, &_new)
	if err != nil {
		return err
	}

	if _old.IsEqua(_new) {
		return nil
	}

	gc.Gcfg = _new

	if gc.OnUpdate != nil {
		gc.OnUpdate(gc.Gcfg)
	}
	return nil
}

// updateService 更新服务
func (gc *GConfig) UpdateService(ctx context.Context, intervale time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			<-time.After(intervale)
			if err := gc.Get(); err != nil {
				log.Println("[gconfig] error", err.Error())
			}
		}
	}
}
func (gc *GConfig) MD5() string {
	md5ec := md5.New()
	md5ec.Reset()
	buf := bytes.NewBuffer([]byte{})
	ec := gob.NewEncoder(buf)
	ec.Encode(gc.Gcfg)
	md5ec.Write(buf.Bytes())
	return hex.EncodeToString(md5ec.Sum(nil))
}

func (gc *GConfig) Load() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), ".gconfig"), os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("[gconfig]%s\n", err.Error())
		return
	}
	defer fl.Close()
	dc := json.NewDecoder(fl)
	err = dc.Decode(&gc.Gcfg)
	if err != nil {
		log.Printf("[gconfig]%s\n", err.Error())
		return
	}
}

func (gc *GConfig) Save() {
	fl, err := os.OpenFile(path.Join(util.GetYTFSPath(), ".gconfig"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[gconfig]%s\n", err.Error())
		return
	}
	defer fl.Close()
	ec := json.NewEncoder(fl)
	err = ec.Encode(&gc.Gcfg)
	if err != nil {
		log.Printf("[gconfig]%s\n", err.Error())
		return
	}
}

func NewGConfig() *GConfig {

	var gc = GConfig{
		Gcfg: Gcfg{
			MaxToken:              500,
			MinToken:              1,
			TTL:                   10,
			Increase:              30,
			RXIncreaseThreshold:   95,
			TXIncreaseThreshold:   95,
			Decrease:              5,
			RXDecreaseThreshold:   80,
			TXDecreaseThreshold:   80,
			TokenWait:             800,
			TokenReturnWait:       800,
			OutlineTimeRange:      600,
			BanTime:               180,
			ElkReport:             false,
			ElkReport2:            true,
			RXTestNum:             2,
			TXTestNum:             2,
			RandDownloadGroupSize: 58,
			DiskTimeout:           5000,
			NodeListUpdateTime:    10,
			RXTestSleep:           100,
			TXTestSleep:           100,
			GcOpen:                true,
			SliceCompareOpen:      true,
		},
		OnUpdate: nil,
	}

	gc.Load()
	return &gc
}

var Gconfig = NewGConfig()

func init() {
	if isDev := os.Getenv("ytfs_dev"); isDev != "" {
		update_url = "http://dnapi.yottachain.net/config/dnconfig_dev.json"
		//log.Println("dev mode")
		if isDev == "1" {
			IsDev = 1
		}
		if isDev == "2" {
			IsDev = 2
		}
		if isDev == "3"{
			IsDev = 3
		}
	}
}
