package main

import (
	"log"

	"github.com/wereliang/cmq_pulsar/pkg/api"
	"github.com/wereliang/cmq_pulsar/pkg/cpadapter"
)

type TopicHandler struct {
	CommHandler
}

func (t *TopicHandler) PublishMessage(ctx CmqContext) {
	req, rsp := &api.PublishMessageRequest{}, &api.PublishMessageResponse{}
	if !t.Parse(ctx, req) {
		return
	}

	log.Printf("publish: %#v", req)

	err := Producer().Send(req.TopicName, []byte(req.MsgBody), nil)
	if err != nil {
		t.WriteError(ctx, ErrInternal)
		return
	}
	t.WriteResp(ctx, rsp)
}

func (t *TopicHandler) BatchPublishMessage(ctx CmqContext) {
	req, rsp := &api.BatchPublishMessageRequest{}, &api.BatchPublishMessageResponse{}
	if !t.Parse(ctx, req) {
		return
	}
	log.Printf("publish: %#v", req)

	var msgs [][]byte
	for _, msg := range req.MsgBody {
		if msg == "" {
			break
		}
		msgs = append(msgs, []byte(msg))
	}
	err := Producer().BatchSend(req.TopicName, msgs)
	if err != nil {
		t.WriteError(ctx, ErrInternal)
		return
	}
	t.WriteResp(ctx, rsp)
}

func (t *TopicHandler) CreateTopic(ctx CmqContext) {
	req, rsp := &api.CreateTopicRequest{}, &api.CreateTopicResponse{}
	if !t.Parse(ctx, req) {
		return
	}

	err := Meta().CreateTopic(req.TopicName)
	if err != nil {
		if err == cpadapter.ErrExist {
			t.WriteError(ctx, ErrQueueExist)
			return
		}
		t.WriteError(ctx, ErrInternal)
		return
	}
	t.WriteResp(ctx, rsp)
}

func (t *TopicHandler) ListTopic(ctx CmqContext) {
	req, rsp := &api.ListTopicRequest{}, &api.ListTopicResponse{}
	if !t.Parse(ctx, req) {
		return
	}

	log.Printf("listtopic: %#v", req)

	if req.Limit > 1000 {
		t.WriteError(ctx, ErrOutOfRange, "limit")
		return
	}

	all, err := Meta().ListTopic()
	if err != nil {
		t.WriteError(ctx, ErrInternal)
		return
	}

	rsp.TotalCount = len(all)
	if req.Offset <= len(all)-1 {
		var topics []*cpadapter.CMQTopic
		if req.Offset+req.Limit > len(all) {
			topics = all[req.Offset:]
		} else {
			topics = all[req.Offset : req.Offset+req.Limit]
		}

		for _, t := range topics {
			rsp.TopicList = append(rsp.TopicList, api.TopicObject{TopicName: t.Name})
		}
	}
	t.WriteResp(ctx, rsp)
}
