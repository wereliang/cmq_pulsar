package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/wereliang/cmq_pulsar/pkg/api"
)

func newCmqApi(tp ...*http.Transport) api.CmqApi {
	secretID := os.Getenv("SecretID")
	secretKey := os.Getenv("SecretKey")
	if secretID == "" || secretKey == "" {
		panic("please export env: SecretID and SecretKey")
	}
	queueURL := os.Getenv("QueueURL")
	topicURL := os.Getenv("TopicURL")
	if queueURL == "" || topicURL == "" {
		panic("please export env: QueueURL and TopicURL")
	}
	var transport *http.Transport
	if len(tp) > 0 {
		transport = tp[0]
	}

	return api.NewCmqApi(api.CmqConfig{
		QueueURL:        queueURL,
		TopicURL:        topicURL,
		Region:          "gz",
		SecretId:        secretID,
		SecretKey:       secretKey,
		SignatureMethod: "HmacSHA256",
	}, transport)
}

func printObject(obj interface{}) {
	data, _ := json.Marshal(obj)
	fmt.Println(string(data))
}
