package util

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/yottachain/YTElkProducer"
	"github.com/yottachain/YTElkProducer/conf"
)

func NewElkClient(tbstr string, open *bool) YTElkProducer.Client {
	elkConf := elasticsearch.Config{
		Addresses: []string{"https://es-hxwjwkzw.public.tencentelasticsearch.com:9200"},
		Username:  "elastic",
		Password:  "yotta@2021",
	}

	ytESConfig := conf.YTESConfig{
		ESConf:      elkConf,
		DebugMode:   true,
		IndexPrefix: tbstr,
		IndexType:   "log",
	}

	if *open {
		client, _ := YTElkProducer.NewClient(ytESConfig)
		//if err != nil {
		//	fmt.Println("error:", err)
		//}
		return client
	}
	return nil
}
