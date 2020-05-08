package statistics

import (
	"encoding/json"
	"sync"
)

type Stat struct {
	SaveRequestCount uint64 `json:"SaveRequestCount"`
	SaveSuccessCount uint64 `json:"SaveSuccessCount"`
	sync.RWMutex
}

func (s *Stat) AddSaveRequestCount() {
	s.Lock()
	defer s.Unlock()

	s.SaveRequestCount = s.SaveRequestCount + 1
}
func (s *Stat) AddSaveSuccessCount() {
	s.Lock()
	defer s.Unlock()

	s.SaveSuccessCount = s.SaveSuccessCount + 1
}

func (s *Stat) JsonEncode() []byte {
	var res []byte

	s.RLock()
	defer s.RUnlock()
	so := *s

	buf, err := json.Marshal(so)
	if err == nil {
		res = buf
	}

	return res
}

func (s *Stat) String() string {
	var res = ""

	buf := s.JsonEncode()
	if buf != nil {
		res = string(buf)
	}

	return res
}

var DefaultStat Stat

func InitDefaultStat() {

	//go func() {
	//	fl, err := os.OpenFile(".stat", os.O_CREATE|os.O_RDONLY, 0644)
	//	if err != nil {
	//		log.Println("[stat]", err.Error())
	//		return
	//	}
	//
	//	buf, err := ioutil.ReadAll(fl)
	//	if err != nil {
	//		log.Println("[stat]", err.Error())
	//		return
	//	}
	//	fl.Close()
	//
	//	if len(buf) > 0 {
	//		var ns Stat
	//		if err := json.Unmarshal(buf, &ns); err != nil {
	//			log.Println("[stat]", err.Error())
	//			return
	//		}
	//		DefaultStat = ns
	//	}
	//}()
	//
	//go func() {
	//	for {
	//		<-time.After(time.Second)
	//		fl2, err := os.OpenFile(".stat", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	//		buf := DefaultStat.JsonEncode()
	//		if err != nil || buf == nil {
	//			log.Println("[stat] write", err.Error())
	//		} else {
	//			fl2.Write(buf)
	//		}
	//
	//		fl2.Close()
	//	}
	//}()
}
