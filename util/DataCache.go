package util

import (
	"sync"
	"time"
)

type DataCache struct {
	ttl        time.Duration
	updateTime time.Time
	data       interface{}
	getData    func() interface{}
	OnUpdate   func(dc *DataCache)
	sync.Mutex
}

func NewDataCache(ttl time.Duration, getDataFunc func() interface{}) *DataCache {
	return &DataCache{ttl: ttl, getData: getDataFunc, data: nil}
}

func (dc *DataCache) Get() interface{} {
	dc.Lock()
	defer dc.Unlock()
	if time.Now().Sub(dc.updateTime) > dc.ttl {
		dc.data = dc.getData()
		dc.updateTime = time.Now()
		if dc.OnUpdate != nil {
			dc.OnUpdate(dc)
		}
	}
	return dc.data
}
