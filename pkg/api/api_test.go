package api

import (
	"log"
	"testing"
)

var capi = NewCmqApi(CmqConfig{
	QueueURL:        "http://cmq-queue-gz.api.qcloud.com/v2/index.php",
	TopicURL:        "http://cmq-topic-gz.api.qcloud.com/v2/index.php",
	Region:          "gz",
	SecretId:        "",
	SecretKey:       "",
	SignatureMethod: "HmacSHA256",
})

func TestListQueue(t *testing.T) {
	resp, err := capi.ListQueue(&ListQueueRequest{Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%#v", resp)
}
