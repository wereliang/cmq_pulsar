package main

import (
	"log"

	"github.com/wereliang/cmq_pulsar/pkg/api"
)

type SubscribeHandler struct {
	CommHandler
}

func (s *SubscribeHandler) CreateSubscribe(ctx CmqContext) {
	req, rsp := &api.CreateSubscribeRequest{}, &api.CreateSubscribeResponse{}
	if !s.Parse(ctx, req) {
		return
	}

	var tags []string
	if req.FilterTag0 != "" {
		tags = append(tags, req.FilterTag0)
	}
	if req.FilterTag1 != "" {
		tags = append(tags, req.FilterTag1)
	}
	if req.FilterTag2 != "" {
		tags = append(tags, req.FilterTag2)
	}
	if req.FilterTag3 != "" {
		tags = append(tags, req.FilterTag3)
	}
	if req.FilterTag4 != "" {
		tags = append(tags, req.FilterTag4)
	}

	err := Meta().CreateSubscribe(req.SubscriptionName, req.Endpoint, req.TopicName, tags)
	if err != nil {
		log.Printf("create subscribe error. %s", err)
		s.WriteError(ctx, ErrInternal)
		return
	}
	s.WriteResp(ctx, rsp)
}

func (s *SubscribeHandler) ListSubscribeByTopic(ctx CmqContext) {
	req, rsp := &api.ListSubscribeRequest{}, &api.ListSubscribeResponse{}
	if !s.Parse(ctx, req) {
		return
	}

	topic := Meta().QueryTopic(req.TopicName)
	if topic == nil {
		s.WriteError(ctx, ErrTopicNotExist)
		return
	}

	rsp.TotalCount = len(topic.Subscribes)
	for _, sub := range topic.Subscribes {
		rsp.SubscriptionList = append(rsp.SubscriptionList, api.SubscribeObject{
			SubscriptionName: sub.Name,
			Endpoint:         sub.Queue,
			Protocol:         "queue",
		})
	}
	s.WriteResp(ctx, rsp)
}

func (s *SubscribeHandler) Unsubscribe(ctx CmqContext) {
	req, rsp := &api.UnsubscribeRequest{}, &api.UnsubscribeResponse{}
	if !s.Parse(ctx, req) {
		return
	}

	err := Meta().DeleteSubscribe(req.SubscriptionName)
	if err != nil {
		s.WriteError(ctx, ErrInternal)
		return
	}
	s.WriteResp(ctx, rsp)
}
