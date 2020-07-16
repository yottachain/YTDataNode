package config

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/yottachain/YTDataNode/logger"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"time"
)

var update_url = "http://dnapi.yottachain.net/config/dnconfig.json"

type UpdateHandler func(gc Gcfg)

type Gcfg struct {
	MaxToken      int           `json:"MaxToken"`
	MinToken      int           `json:"MinToken"`
	Increase      int           `json:"Increase"`
	Decrease      int           `json:"Decrease"`
	MaxConn       int           `json:"MaxConn"`
	TokenInterval time.Duration `json:"TokenInterval"`
	TTL           time.Duration `json:"TTL"`
	//AllowBack     bool          `json:"AllowBack"`
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

func NewGConfig() *GConfig {
	var gc GConfig
	gc.MaxToken = 500
	gc.MinToken = 20
	gc.TokenInterval = 10
	gc.Increase = 5
	gc.Decrease = 30
	gc.MaxConn = 500
	gc.TTL = 10
	return &gc
}
