package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	log "github.com/yottachain/YTDataNode/logger"
	"net/http"
	"strings"
)

type ErrShard struct {
	Shard []byte	`json:"err_shard"`
	RebuildStatus int32	`json:"rebuild_status"`
}

type VerifyErrShards struct {
	MinerId int64	`json:"miner_id"`
	ErrShards [] ErrShard	`json:"err_shards"`
	ErrNums int32	`json:"err_nums"`
}

var client *elasticsearch.Client

func GetElasticClinet() *elasticsearch.Client {
	if client == nil {
		addresses := []string{"http://10.0.26.141:9200"}
		config := elasticsearch.Config{
			Addresses: addresses,
			Username:  "elastic",
			Password:  "7uji9olp-",
			CACert: nil,
		}
		cli, err := elasticsearch.NewClient(config)
		if cli == nil {
			log.Println("verify elastic GetElasticClinet NewClient fail:", err)
			return nil
		}
		log.Println("verify elastic GetElasticClinet Success")
		client = cli
	}
	return client
}

func PutVerifyErrData(src *VerifyErrShards) error{
	client := GetElasticClinet()
	if nil == client {
		log.Printf("verify get elastic client fail")
		return fmt.Errorf("verify get elastic client fail")
	}

	body, _ := json.Marshal(src)

	resp, err := client.Index("verifyErr", strings.NewReader(string(body)),
		client.Index.WithContext(context.Background()),
		client.Index.WithRefresh("true"))
	if err != nil {
		log.Println("verify write data error %s", err.Error())
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		log.Println("verify write data  response status %d\n", resp.StatusCode)
	}else {
		log.Println("verify write data  response status %d\n", resp.StatusCode)
		return fmt.Errorf("resp status not 200+ OK")
	}

	return nil
}