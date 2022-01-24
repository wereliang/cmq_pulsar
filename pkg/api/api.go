package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
)

type CmqApi interface {
	CreateTopic(*CreateTopicRequest) (*CreateTopicResponse, error)
	CreateQueue(*CreateQueueRequest) (*CreateQueueResponse, error)
	CreateSubscribe(*CreateSubscribeRequest) (*CreateSubscribeResponse, error)

	ListQueue(*ListQueueRequest) (*ListQueueResponse, error)
	ListTopic(*ListTopicRequest) (*ListTopicResponse, error)
	ListSubscribe(*ListSubscribeRequest) (*ListSubscribeResponse, error)
	Unsubscribe(*UnsubscribeRequest) (*UnsubscribeResponse, error)

	PublishMessage(*PublishMessageRequest) (*PublishMessageResponse, error)
	BatchPublishMessage(*BatchPublishMessageRequest) (*BatchPublishMessageResponse, error)
	ReceiveMessage(*ReceiveMessageRequest) (*ReceiveMessageResponse, error)
	BatchReceiveMessage(*BatchReceiveMessageRequest) (*BatchReceiveMessageResponse, error)
	DeleteMessage(*DeleteMessageRequest) (*DeleteMessageResponse, error)
}

var defaultTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	MaxIdleConns:          100,
	MaxConnsPerHost:       100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

func NewCmqApi(opts CmqConfig, transport *http.Transport) CmqApi {
	api := &cmqApi{
		builder: NewCMQRequestBuilder(opts),
		opts:    opts,
	}
	if transport == nil {
		transport = defaultTransport
	}
	api.client = &http.Client{
		Transport: transport,
	}
	return api
}

type cmqApi struct {
	builder CMQRequestBuilder
	opts    CmqConfig
	client  *http.Client
}

func (api *cmqApi) CreateTopic(req *CreateTopicRequest) (*CreateTopicResponse, error) {
	response := &CreateTopicResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "CreateTopic", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) CreateQueue(req *CreateQueueRequest) (*CreateQueueResponse, error) {
	response := &CreateQueueResponse{}
	if err := api.do(
		api.opts.QueueURL,
		api.builder.Build(api.opts.QueueURL, "CreateQueue", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) CreateSubscribe(req *CreateSubscribeRequest) (*CreateSubscribeResponse, error) {
	response := &CreateSubscribeResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "Subscribe", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) Unsubscribe(req *UnsubscribeRequest) (*UnsubscribeResponse, error) {
	response := &UnsubscribeResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "Unsubscribe", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) ListQueue(req *ListQueueRequest) (*ListQueueResponse, error) {
	response := &ListQueueResponse{}
	if err := api.do(
		api.opts.QueueURL,
		api.builder.Build(api.opts.QueueURL, "ListQueue", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) ListTopic(req *ListTopicRequest) (*ListTopicResponse, error) {
	response := &ListTopicResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "ListTopic", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) ListSubscribe(req *ListSubscribeRequest) (*ListSubscribeResponse, error) {
	response := &ListSubscribeResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "ListSubscriptionByTopic", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) PublishMessage(req *PublishMessageRequest) (*PublishMessageResponse, error) {
	response := &PublishMessageResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "PublishMessage", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) BatchPublishMessage(req *BatchPublishMessageRequest) (*BatchPublishMessageResponse, error) {
	response := &BatchPublishMessageResponse{}
	if err := api.do(
		api.opts.TopicURL,
		api.builder.Build(api.opts.TopicURL, "BatchPublishMessage", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) ReceiveMessage(req *ReceiveMessageRequest) (*ReceiveMessageResponse, error) {
	response := &ReceiveMessageResponse{}
	if err := api.do(
		api.opts.QueueURL,
		api.builder.Build(api.opts.QueueURL, "ReceiveMessage", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) BatchReceiveMessage(req *BatchReceiveMessageRequest) (*BatchReceiveMessageResponse, error) {
	response := &BatchReceiveMessageResponse{}
	if err := api.do(
		api.opts.QueueURL,
		api.builder.Build(api.opts.QueueURL, "BatchReceiveMessage", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) DeleteMessage(req *DeleteMessageRequest) (*DeleteMessageResponse, error) {
	response := &DeleteMessageResponse{}
	if err := api.do(
		api.opts.QueueURL,
		api.builder.Build(api.opts.QueueURL, "DeleteMessage", req),
		response); err != nil {
		return nil, err
	}
	return response, nil
}

func (api *cmqApi) do(target string, request string, response interface{}) error {
	r, err := api.client.Post(target, "application/x-www-form-urlencoded", strings.NewReader(request))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, response)
	if err != nil {
		return fmt.Errorf("json unmarshal fail.[%s] [%s]", err, string(b))
	}
	return nil
}
