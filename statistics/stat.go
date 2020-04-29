package statistics

import (
	"encoding/json"
	log "github.com/yottachain/YTDataNode/logger"
	"io/ioutil"
	"os"
	"sync/atomic"
	"time"
)

type Stat struct {
	SaveRequestCount uint64 `json:"SaveRequestCount"`
	SaveSuccessCount uint64 `json:"SaveSuccessCount"`
}

func (s *Stat) AddSaveRequestCount() {
	atomic.AddUint64(&s.SaveRequestCount, 1)
}
func (s *Stat) AddSaveSuccessCount() {
	atomic.AddUint64(&s.SaveSuccessCount, 1)
}

func (s *Stat) String() string {
	var res = ""

	so := *s

	buf, err := json.Marshal(so)
	if err == nil {
		res = string(buf)
	}

	return res
}

var DefaultStat Stat

func init() {
	fl, err := os.OpenFile(".stat", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("[stat]", err.Error())
		return
	}

	buf, err := ioutil.ReadAll(fl)
	if err != nil {
		log.Println("[stat]", err.Error())
		return
	}

	if len(buf) > 0 {
		var ns Stat
		if err := json.Unmarshal(buf, &ns); err != nil {
			log.Println("[stat]", err.Error())
			return
		}
		DefaultStat = ns
	}

	go func() {
		for {
			<-time.After(10 * time.Second)
			buf, err := json.Marshal(DefaultStat)
			if err != nil {
				log.Println("[stat] write", err.Error())
			} else {
				fl.Write(buf)
			}
		}
	}()
}
