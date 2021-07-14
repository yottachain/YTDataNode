package util

// import (
// 	"github.com/elastic/go-elasticsearch/v8"
// 	"github.com/yottachain/YTElkProducer"
// 	"github.com/yottachain/YTElkProducer/conf"
// )

// func NewElkClient(tbstr string, open *bool) YTElkProducer.Client {
// 	elkConf := elasticsearch.Config{
// 		Addresses: []string{"https://elk1-nm.yottachain.net"},
// 		Username:  "elastic",
// 		Password:  "yotta_2021",
// 	}

// 	ytESConfig := conf.YTESConfig{
// 		ESConf:      elkConf,
// 		DebugMode:   true,
// 		IndexPrefix: tbstr,
// 		IndexType:   "log",
// 	}

// 	if *open {
// 		client, _ := YTElkProducer.NewClient(ytESConfig)
// 		//if err != nil {
// 		//	fmt.Println("error:", err)
// 		//}
// 		return client
// 	}
// 	return nil
// }
