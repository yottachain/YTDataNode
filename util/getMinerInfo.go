package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type MinerInfo struct {
	ID            uint64 `json:"_id"`
	Quota         uint64 `json:"quota"`
	MaxDataSpace  uint64 `json:"maxDataSpace"`
	AssignedSpace uint64 `json:"assignedSpace"`
	updateTime    time.Time
}

func GetMinerInfo(id uint64) *MinerInfo {

	buf := bytes.NewBuffer([]byte{})
	buf.WriteString(fmt.Sprintf(`{"_id":%d}`, id))
	resp, err := http.Post("https://dnrpc.yottachain.net/query", "application/json", buf)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer resp.Body.Close()

	var res []MinerInfo
	dc := json.NewDecoder(resp.Body)
	dc.Decode(&res)

	if res == nil || len(res) <= 0 {
		return nil
	} else {
		return &res[0]
	}
}

func (mi *MinerInfo) IsNoSpace(used uint64) bool {
	if time.Now().Sub(mi.updateTime) > time.Minute*5 {
		_mi := GetMinerInfo(mi.ID)
		if mi == nil || _mi == nil {
			return false
		}
		mi.AssignedSpace = _mi.AssignedSpace
		mi.Quota = _mi.Quota
		mi.MaxDataSpace = _mi.MaxDataSpace
		mi.updateTime = time.Now()
	}
	return mi.MaxDataSpace-used <= 655360 || mi.AssignedSpace-used <= 655360 || mi.Quota-used <= 655360
}
