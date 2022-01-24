package api

import (
	"fmt"
	"log"
	"net/url"
	"testing"
)

func TestBuilder(t *testing.T) {
	req := &CreateQueueRequest{
		MaxMsgHeapNum:     2000,
		VisibilityTimeout: 600,
	}
	builder := &cmqRequestBuilder{}
	fmt.Println(builder.Build("http://www.qq.com", "create", req))
}

func TestArray(t *testing.T) {
	req := &BatchPublishMessageRequest{
		TopicName: "test",
		MsgBody:   []string{"aa", "bb"},
	}
	builder := &cmqRequestBuilder{}
	fmt.Println(builder.Build("http://www.qq.com", "create", req))
}

func TestParser(t *testing.T) {
	values, err := url.ParseQuery("Action=ListQueue&Nonce=307450928&RequestClient=SDK_CPP_1.3&SecretId=&Signature=%3D&SignatureMethod=HmacSHA256&Timestamp=1641451136&limit=20&offset=20")
	if err != nil {
		log.Fatalln(err)
	}
	req := &ListQueueRequest{}
	builder := &cmqRequestBuilder{}
	err = builder.Parse(values, req)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%#v\n", req)

	req2 := &ListQueueRequest{}
	values, err = url.ParseQuery("Action=ListQueue&Nonce=307450928&RequestClient=SDK_CPP_1.3&SecretId=&Signature=%3D&SignatureMethod=HmacSHA256&Timestamp=1641451136&limit=20")
	err = builder.Parse(values, req2)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%#v\n", req2)

	req3 := &BatchPublishMessageRequest{}
	values, err = url.ParseQuery("Action=ListQueue&Nonce=307450928&RequestClient=SDK_CPP_1.3&SecretId=&Signature=%3D&SignatureMethod=HmacSHA256&Timestamp=1641451136&topicName=test&msgBody.0=message1&msgBody.1=message2&msgBody.4=message4")
	err = builder.Parse(values, req3)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%#v\n", req3)
}
