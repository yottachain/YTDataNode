package statistics

import (
	"encoding/json"
	"os"
	"time"
)

type LastUpTime struct {
	LT time.Time
}

func (lt *LastUpTime) Read() time.Time {
	defer func() {
		fl, err := os.OpenFile(".lastUptime", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err == nil {
			defer fl.Close()
			ec := json.NewEncoder(fl)
			ec.Encode(lt)
		}
	}()
	fl, err := os.OpenFile(".lastUptime", os.O_RDONLY, 0644)
	if err != nil {
		return time.Time{}
	}
	dc := json.NewDecoder(fl)
	dc.Decode(lt)
	return lt.LT
}
