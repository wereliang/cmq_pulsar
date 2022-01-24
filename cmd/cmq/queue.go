package main

import (
	"log"
	"time"

	"github.com/wereliang/cmq_pulsar/pkg/api"
	"github.com/wereliang/cmq_pulsar/pkg/cpadapter"
)

type QueueHandler struct {
	CommHandler
}

func (q *QueueHandler) CreateQueue(ctx CmqContext) {
	req, rsp := &api.CreateQueueRequest{}, &api.CreateQueueResponse{}
	if !q.Parse(ctx, req) {
		return
	}

	err := Meta().CreateQueue(req.QueueName)
	if err != nil {
		if err == cpadapter.ErrExist {
			q.WriteError(ctx, ErrQueueExist)
			return
		}
		q.WriteError(ctx, ErrInternal)
		return
	}
	q.WriteResp(ctx, rsp)
}

func (q *QueueHandler) ListQueue(ctx CmqContext) {
	req, rsp := &api.ListQueueRequest{}, &api.ListQueueResponse{}
	if !q.Parse(ctx, req) {
		return
	}

	if req.Limit > 1000 {
		q.WriteError(ctx, ErrOutOfRange, "limit")
		return
	}

	all, err := Meta().ListQueue()
	if err != nil {
		q.WriteError(ctx, ErrInternal)
		return
	}

	rsp.TotalCount = len(all)
	if req.Offset <= len(all)-1 {
		var queues []*cpadapter.CMQQueue
		if req.Offset+req.Limit > len(all) {
			queues = all[req.Offset:]
		} else {
			queues = all[req.Offset : req.Offset+req.Limit]
		}

		for _, q := range queues {
			rsp.QueueList = append(rsp.QueueList, api.QueueObject{QueueName: q.Name})
		}
	}
	q.WriteResp(ctx, rsp)
}

func (q *QueueHandler) ReceiveMessage(ctx CmqContext) {
	req, rsp := &api.ReceiveMessageRequest{}, &api.ReceiveMessageResponse{}
	if !q.Parse(ctx, req) {
		return
	}

	// 未传则按队列属性的默认设置，这里统一为15s
	if ctx.Values().Get("pollingWaitSeconds") == "" {
		req.PollingWaitSeconds = 15
	}

	msg, err := Consumer().Receive(req.QueueName, time.Duration(req.PollingWaitSeconds)*time.Second)
	if err != nil {
		if err == cpadapter.ErrNoMessage {
			q.WriteError(ctx, ErrNoMessage)
			return
		}

		time.Sleep(time.Second)
		if err == cpadapter.ErrQueueNoSubscribe {
			q.WriteError(ctx, ErrQueueNoSubscription)
		} else {
			q.WriteError(ctx, ErrInternal)
		}
		return
	}
	log.Printf("receive: %#v", msg)

	rsp.MsgBody = string(msg.MsgBody())
	rsp.DequeueCount = int(msg.DequeueCount())
	rsp.ReceiptHandle = msg.ReceiptHandle()
	rsp.MsgId = msg.MsgID()
	q.WriteResp(ctx, rsp)
}

func (q *QueueHandler) BatchReceiveMessage(ctx CmqContext) {
	req, rsp := &api.BatchReceiveMessageRequest{}, &api.BatchReceiveMessageResponse{}
	if !q.Parse(ctx, req) {
		return
	}
	// 未传则按队列属性的默认设置，这里统一为15s
	if ctx.Values().Get("pollingWaitSeconds") == "" {
		req.PollingWaitSeconds = 15
	}

	msgs, err := Consumer().BatchReceive(req.QueueName, time.Duration(req.PollingWaitSeconds)*time.Second, req.NumOfMsg)
	if err != nil {
		if err == cpadapter.ErrNoMessage {
			q.WriteError(ctx, ErrNoMessage)
			return
		}

		time.Sleep(time.Second)
		if err == cpadapter.ErrQueueNoSubscribe {
			q.WriteError(ctx, ErrQueueNoSubscription)
		} else {
			q.WriteError(ctx, ErrInternal)
		}
		return
	}
	log.Printf("receive: %#v", msgs)

	for _, msg := range msgs {
		rsp.MsgInfoList = append(rsp.MsgInfoList, api.MessageResponse{
			MsgBody:       string(msg.MsgBody()),
			DequeueCount:  int(msg.DequeueCount()),
			ReceiptHandle: msg.ReceiptHandle(),
			MsgId:         msg.MsgID(),
		})
	}
	q.WriteResp(ctx, rsp)
}

func (q *QueueHandler) DeleteMessage(ctx CmqContext) {
	req, rsp := &api.DeleteMessageRequest{}, &api.DeleteMessageResponse{}
	if !q.Parse(ctx, req) {
		return
	}

	err := Consumer().Delete(req.QueueName, req.ReceiptHandle)
	if err != nil {
		q.WriteError(ctx, ErrInternal)
		return
	}
	q.WriteResp(ctx, rsp)
}
