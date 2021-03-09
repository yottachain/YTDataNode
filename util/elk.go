package util

import (
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/yottachain/YTElkProducer"
	"github.com/yottachain/YTElkProducer/conf"
)

func NewElkClient(tbstr string) YTElkProducer.Client {
	elkConf := elasticsearch.Config{
		Addresses: []string{"https://es-kljdi4to.public.tencentelasticsearch.com:9200"},
		Username:  "elastic",
		Password:  "yotta@2021",
	}

	ytESConfig := conf.YTESConfig{
		ESConf:      elkConf,
		DebugMode:   true,
		IndexPrefix: tbstr,
		IndexType:   "log",
	}

	client, err := YTElkProducer.NewClient(ytESConfig)
	if err != nil {
		fmt.Println(err)
	}
	return client
}
