package config

import (
	"context"
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

type UpdateHandler func(gc Gcfg)

type Gcfg struct {
	MaxToken          int   `json:"MaxToken"`
	MinToken          int   `json:"MinToken"`
	TTL               int64 `json:"TTL"`
	Increase          int64 `json:"Increase"`
	IncreaseThreshold int64 `json:"IncreaseThreshold"`
	Decrease          int64 `json:"Decrease"`
	DecreaseThreshold int64 `json:"DecreaseThreshold"`
	TokenWait         int64 `json:"TokenWait"`
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
	log.Printf("[gconfig]读取配置 %v\n", gc.Gcfg)
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
			MaxToken:          500,
			MinToken:          1,
			TTL:               10,
			Increase:          30,
			IncreaseThreshold: 95,
			Decrease:          5,
			DecreaseThreshold: 80,
			TokenWait:         1000,
		},
		OnUpdate: nil,
	}

	gc.Load()
	return &gc
}

var Gconfig = NewGConfig()

func init() {
}
