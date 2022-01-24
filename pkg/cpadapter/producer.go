package cpadapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var (
	ErrProducerClosed = errors.New("producer closed")
)

type CmqPulsarProducer interface {
	Send(topic string, message []byte, tags []string) error
	BatchSend(topic string, message [][]byte) error
	Close()
}

func newCmqPulsarProducer(client pulsar.Client, meta CmqPulsarMetaServer) (CmqPulsarProducer, error) {
	cp := &cmqPulsarProducer{
		client:   client,
		produces: make(map[string]Producer),
		meta:     meta,
		ch:       make(chan *MetaEvent, 1024),
	}
	go cp.watchMeta()
	cp.meta.AddWatch(cp.ch)
	return cp, nil
}

type cmqPulsarProducer struct {
	sync.RWMutex
	ch       chan *MetaEvent
	client   pulsar.Client
	produces map[string]Producer
	meta     CmqPulsarMetaServer
}

func (adp *cmqPulsarProducer) watchMeta() {
	// lazy reload. TODO: reload here?
	for evt := range adp.ch {
		if evt.Type == eventTopic {
			log.Printf("new event: %#v", evt)
			adp.Lock()
			if c, ok := adp.produces[evt.Name]; ok {
				go func() { c.Close() }()
				delete(adp.produces, evt.Name)
			}
			adp.Unlock()
		}
	}
}

func (adp *cmqPulsarProducer) getProducer(topic string) (Producer, error) {
	adp.RLock()
	if p, ok := adp.produces[topic]; ok {
		adp.RUnlock()
		return p, nil
	}
	adp.RUnlock()

	tmeta := adp.meta.QueryTopic(topic)
	if tmeta != nil {
		produce, err := newProducer(
			adp.client,
			MakePulsarTopic(adp.meta.Admin().Cluster(), topic),
		)
		if err != nil {
			return nil, err
		}
		adp.Lock()
		defer adp.Unlock()
		adp.produces[topic] = produce
		return produce, nil
	}
	return nil, fmt.Errorf("topic %s not exist", topic)

}

func (adp *cmqPulsarProducer) Send(topic string, message []byte, tags []string) error {
	p, err := adp.getProducer(topic)
	if err != nil {
		return err
	}
	return p.Send(message, tags)
}

func (adp *cmqPulsarProducer) Close() {
	adp.Lock()
	defer adp.Unlock()
	for _, v := range adp.produces {
		v.Close()
	}
}

func (adp *cmqPulsarProducer) BatchSend(topic string, messages [][]byte) error {
	p, err := adp.getProducer(topic)
	if err != nil {
		return err
	}
	return p.BatchSend(messages)
}

type Producer interface {
	Send(message []byte, tags []string) error
	BatchSend(message [][]byte) error
	Close()
}

func newProducer(client pulsar.Client, name string) (Producer, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		// DisableBatching: true,
		Topic:                   name,
		BatchingMaxPublishDelay: time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &producerImpl{producer: producer}, nil
}

type producerImpl struct {
	sync.RWMutex
	producer pulsar.Producer
}

func (p *producerImpl) Close() {
	p.Lock()
	defer p.Unlock()
	p.producer.Close()
	p.producer = nil
}

func (p *producerImpl) Send(message []byte, tags []string) error {
	var properties map[string]string
	if len(tags) != 0 {
		properties = make(map[string]string)
		stags := strings.Join(tags, "|")
		properties[CMQTag] = stags
		// log.Printf("send with tag:%s", stags)
	}

	p.RLock()
	defer p.RUnlock()
	if p.producer == nil {
		return ErrProducerClosed
	}

	_, err := p.producer.Send(
		context.Background(),
		&pulsar.ProducerMessage{
			Payload:    message,
			Properties: properties,
		})
	return err
}

func (p *producerImpl) BatchSend(messages [][]byte) error {
	p.RLock()
	defer p.RUnlock()
	if p.producer == nil {
		return ErrProducerClosed
	}

	for idx, msg := range messages {
		seqID := int64(idx)
		p.producer.SendAsync(context.Background(),
			&pulsar.ProducerMessage{
				Payload:    msg,
				SequenceID: &seqID,
			}, func(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
				log.Printf("id:%#v msg:%#v err:%#v", id, msg, err)
			})
	}
	p.producer.Flush()
	return nil
}
