package cpadapter

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var (
	ErrNoMessage         = errors.New("no message")
	ErrQueueNotExist     = errors.New("queue not exist")
	ErrQueueNoSubscribe  = errors.New("queue no subscribe")
	ErrConsumerClosed    = errors.New("consumer closed")
	ErrSubscribeNotExist = errors.New("subscribe not exist")
	ErrInvalidHandleID   = errors.New("invalid handleid")
)

type CmqPulsarConsumer interface {
	Receive(queue string, waitTime time.Duration) (Message, error)
	BatchReceive(queue string, waitTime time.Duration, number int) ([]Message, error)
	Delete(queue string, handleID string) error
	Close()
}

func newCmqPulsarConsumer(client pulsar.Client, meta CmqPulsarMetaServer, opts AdapterOptions) (CmqPulsarConsumer, error) {
	cc := &cmqPulsarConsumer{
		client:    client,
		consumers: make(map[string]ActiveConsumer),
		meta:      meta,
		opts:      opts,
		ch:        make(chan *MetaEvent, 1024),
	}
	go cc.watchMeta()
	go cc.release()
	cc.meta.AddWatch(cc.ch)
	return cc, nil
}

type cmqPulsarConsumer struct {
	sync.RWMutex
	ch        chan *MetaEvent
	opts      AdapterOptions
	client    pulsar.Client
	consumers map[string]ActiveConsumer
	meta      CmqPulsarMetaServer
}

func (adp *cmqPulsarConsumer) watchMeta() {
	// lazy reload. TODO: reload here?
	for evt := range adp.ch {
		if evt.Type == eventQueue {
			log.Printf("new event: %#v", evt)
			adp.Lock()
			if c, ok := adp.consumers[evt.Name]; ok {
				go func() {
					c.Close()
					if evt.Cb != nil {
						evt.Cb()
					}
				}()
				delete(adp.consumers, evt.Name)
			}
			adp.Unlock()
		}
	}
}

func (adp *cmqPulsarConsumer) release() {
	t := time.NewTicker(time.Minute)
	for {
		select {
		case <-t.C:
			var keys []string
			var wg sync.WaitGroup
			adp.RLock()
			for k, c := range adp.consumers {
				if active, ok := c.LastActive(); ok {
					if time.Since(active) > time.Minute {
						keys = append(keys, k)
						wg.Add(1)
						go func(ac ActiveConsumer) { defer wg.Done(); ac.Close() }(c)
					}
				}
			}
			adp.RUnlock()
			wg.Wait()
			adp.Lock()
			for _, k := range keys {
				delete(adp.consumers, k)
				log.Printf("delete consumer: %s", k)
			}
			adp.Unlock()
		}
	}
}

func (adp *cmqPulsarConsumer) getConsumer(queue string) (ActiveConsumer, error) {
	var consumer ActiveConsumer
	var ok bool
	defer func() {
		if consumer != nil {
			consumer.Active()
		}
	}()

	adp.RLock()
	if consumer, ok = adp.consumers[queue]; ok {
		adp.RUnlock()
		return consumer, nil
	}
	adp.RUnlock()

	qmeta := adp.meta.QueryQueue(queue)
	if qmeta != nil {
		if len(qmeta.Subscribes) == 0 {
			return nil, ErrQueueNoSubscribe
		}

		topics, tags := qmeta.GetTopics()

		adp.Lock()
		defer adp.Unlock()
		if consumer, ok = adp.consumers[queue]; ok {
			return consumer, nil
		}
		consumer, _ = newConsumer(adp.client, queue, topics, adp.meta.Admin().Cluster(), tags, &adp.opts)
		adp.consumers[queue] = consumer
		return consumer, nil
	}
	return nil, ErrQueueNotExist
}

func (adp *cmqPulsarConsumer) Receive(queue string, waitTime time.Duration) (Message, error) {
	consumer, err := adp.getConsumer(queue)
	if err != nil {
		return nil, err
	}
	// TODO: retry if consumer closed
	return consumer.Receive(waitTime)
}

func (adp *cmqPulsarConsumer) BatchReceive(queue string, waitTime time.Duration, number int) ([]Message, error) {
	consumer, err := adp.getConsumer(queue)
	if err != nil {
		return nil, err
	}
	return consumer.BatchReceive(waitTime, number)
}

func (adp *cmqPulsarConsumer) Delete(queue string, handleID string) error {
	consumer, err := adp.getConsumer(queue)
	if err != nil {
		return err
	}
	// TODO: retry if consumer closed
	return consumer.Delete(handleID)
}

func (adp *cmqPulsarConsumer) Close() {
	adp.Lock()
	defer adp.Unlock()
	for _, c := range adp.consumers {
		c.Close()
	}
}

type Consumer interface {
	Receive(waitTime time.Duration) (Message, error)
	BatchReceive(waitTime time.Duration, number int) ([]Message, error)
	Delete(handleID string) error
	Close()
}

type ActiveConsumer interface {
	ObjectActive
	Consumer
}

func newConsumer(client pulsar.Client, queue string,
	topics []string, cluster *ClusterInfo, tags CMQTags, opts *AdapterOptions) (ActiveConsumer, error) {

	impl := &consumerImpl{
		name:        queue,
		topics:      topics,
		tags:        tags,
		consumers:   make(map[string]pulsar.Consumer),
		msgChan:     make(chan pulsar.ConsumerMessage, 1024),
		status:      StatusInit,
		cluster:     cluster,
		nackTimeout: time.Duration(opts.NackTimeout) * time.Second,
		client:      client,
	}
	return impl, nil
}

type consumerImpl struct {
	commActive
	sync.RWMutex
	msgChan     chan pulsar.ConsumerMessage
	name        string
	topics      []string
	tags        CMQTags
	consumers   map[string]pulsar.Consumer // topic -> pulsar consumer
	status      Status
	cluster     *ClusterInfo
	nackTimeout time.Duration
	client      pulsar.Client
}

func (c *consumerImpl) getConsumer(topic string) (pulsar.Consumer, error) {
	if consumer, ok := c.consumers[topic]; !ok {
		return nil, ErrSubscribeNotExist
	} else {
		return consumer, nil
	}
}

func (c *consumerImpl) init() error {
	for _, t := range c.topics {
		cc, err := c.client.Subscribe(pulsar.ConsumerOptions{
			Topics:              []string{MakePulsarTopic(c.cluster, t)},
			SubscriptionName:    c.name,
			Type:                pulsar.Shared,
			NackRedeliveryDelay: c.nackTimeout,
			MessageChannel:      c.msgChan,
		})
		if err != nil {
			for _, consumer := range c.consumers {
				consumer.Close()
			}
			return err
		}
		c.consumers[t] = cc
	}
	return nil
}

func (c *consumerImpl) checkStatus() error {
	if c.status == StatusRunning {
		return nil
	}

	c.Lock()
	defer c.Unlock()
	switch c.status {
	case StatusRunning:
		return nil
	case StatusClosed:
		return ErrConsumerClosed
	default:
		err := c.init()
		if err != nil {
			return err
		}
		c.status = StatusRunning
	}
	return nil
}

func (c *consumerImpl) Receive(waitTime time.Duration) (Message, error) {
	err := c.checkStatus()
	if err != nil {
		return nil, err
	}
	c.RLock()
	defer c.RUnlock()
	err = c.checkStatus()
	if err != nil {
		return nil, err
	}

	start := time.Now()
	for {
		if time.Now().Sub(start) > waitTime {
			return nil, ErrNoMessage
		}

		timer := time.NewTimer(waitTime)
		var msg pulsar.ConsumerMessage
		select {
		case <-timer.C:
			return nil, ErrNoMessage
		case msg = <-c.msgChan:
			timer.Stop()
		}

		// ctx, cancel := context.WithTimeout(
		// 	context.Background(), waitTime)
		// defer cancel()
		// msg, err := c.consumer.Receive(ctx)
		// if err != nil {
		// 	if context.DeadlineExceeded == err {
		// 		return nil, ErrNoMessage
		// 	}
		// 	return nil, err
		// }

		cmqTopic := MakeCMQTopic(msg.Topic())
		ack := false
		if c.tags != nil {
			if tags, ok := c.tags[cmqTopic]; ok && tags != nil {
				if strTags, ok := msg.Properties()[CMQTag]; !ok {
					ack = true
				} else {
					// multi tags join by "|"
					msgTags := strings.Split(strTags, "|")
					ack = true
					for _, t := range msgTags {
						if _, ok := tags[t]; ok {
							ack = false
							break
						}
					}
				}
			}
		}

		consumer, err := c.getConsumer(cmqTopic)
		// it can't go here
		if err != nil || consumer == nil {
			log.Printf("not found consumer for topic: %s", cmqTopic)
			return nil, ErrSubscribeNotExist
		}
		if !ack {
			consumer.Nack(msg)
			dequeue := msg.RedeliveryCount() + 1
			handleID := fmt.Sprintf("%s.%s.%d",
				base64.StdEncoding.EncodeToString(msg.ID().Serialize()),
				cmqTopic,
				dequeue)
			return newMessage(handleID, msg.Payload(), MakeMessageID(msg.ID()), dequeue), nil
		}
		consumer.Ack(msg)
	}
}

func (c *consumerImpl) BatchReceive(waitTime time.Duration, number int) ([]Message, error) {
	var messages []Message
	for i := 0; i < number; i++ {
		wt := 100 * time.Millisecond
		if i == 0 {
			wt = waitTime
		}
		msg, err := c.Receive(wt)
		if err != nil {
			if err == ErrNoMessage || err == ErrConsumerClosed {
				break
			} else {
				return nil, err
			}
		}
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		return nil, ErrNoMessage
	}
	return messages, nil
}

func (c *consumerImpl) Delete(handleID string) error {
	res := strings.Split(handleID, ".")
	if len(res) < 2 {
		return ErrInvalidHandleID
	}
	topic := res[1]
	binID, err := base64.StdEncoding.DecodeString(res[0])
	if err != nil {
		panic(fmt.Sprintf("handleID:%s err:%s", handleID, err))
	}
	msgID, err := pulsar.DeserializeMessageID(binID)
	if err != nil {
		return err
	}

	err = c.checkStatus()
	if err != nil {
		return err
	}
	c.RLock()
	defer c.RUnlock()
	err = c.checkStatus()
	if err != nil {
		return err
	}

	consumer, err := c.getConsumer(topic)
	if err != nil {
		return err
	}
	consumer.AckID(msgID)
	return nil
}

func (c *consumerImpl) Close() {
	c.Lock()
	defer c.Unlock()
	log.Printf("consumer %s [%v] closed", c.name, c.topics)
	for k, v := range c.consumers {
		v.Close()
		c.consumers[k] = nil
	}
	c.status = StatusClosed
}
