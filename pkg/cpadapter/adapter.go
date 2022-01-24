package cpadapter

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
)

type AdapterOptions struct {
	PulsarURL   string `yaml:"pulsarUrl"`
	Token       string `yaml:"token"`
	NackTimeout int    `yaml:"nackTimeout"` // second
	Logger      plog.Logger
}

type CmqPulsarAdapter interface {
	Producer() CmqPulsarProducer
	Consumer() CmqPulsarConsumer
	Meta() CmqPulsarMetaServer
	Close()
}

func NewCmqPulsarAdapter(meta CmqPulsarMetaServer, opts AdapterOptions) (CmqPulsarAdapter, error) {
	if opts.NackTimeout == 0 {
		opts.NackTimeout = DefaultNackTimeout
	}

	var auth pulsar.Authentication
	if opts.Token != "" {
		auth = pulsar.NewAuthenticationToken(opts.Token)
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     opts.PulsarURL,
		Authentication:          auth,
		OperationTimeout:        30 * time.Second,
		ConnectionTimeout:       30 * time.Second,
		Logger:                  opts.Logger,
		MaxConnectionsPerBroker: 1024,
	})
	if err != nil {
		return nil, err
	}

	adapter := &cmqPulsarAdapterImpl{
		opts:   opts,
		client: client,
		meta:   meta,
	}
	adapter.producer, _ = newCmqPulsarProducer(client, meta)
	adapter.consumer, _ = newCmqPulsarConsumer(client, meta, opts)
	return adapter, nil
}

func NewCmqPulsarAdapterWithConfig(cfg *Config) (adapter CmqPulsarAdapter, err error) {
	var (
		admin PulsarAdmin
		meta  CmqPulsarMetaServer
	)

	defer func() {
		if err != nil {
			if admin != nil {
				admin.Close()
			}
			if meta != nil {
				meta.Close()
			}
			if adapter != nil {
				adapter.Close()
			}
		}
	}()

	cfg.Check()

	switch cfg.Type {
	case BackendPulsar:
		if admin, err = NewPulsarAdmin(cfg.Admin); err != nil {
			return
		}
	case BackendTDMQ:
		if admin, err = NewTdmqAdmin(cfg.Admin); err != nil {
			return
		}
	}
	if meta, err = NewMetaServerRedis(admin, cfg.Meta); err != nil {
		return
	}
	adapter, err = NewCmqPulsarAdapter(meta, cfg.Adapter)
	return
}

type cmqPulsarAdapterImpl struct {
	opts     AdapterOptions
	client   pulsar.Client
	producer CmqPulsarProducer
	consumer CmqPulsarConsumer
	meta     CmqPulsarMetaServer
}

func (adp *cmqPulsarAdapterImpl) Producer() CmqPulsarProducer {
	return adp.producer
}

func (adp *cmqPulsarAdapterImpl) Consumer() CmqPulsarConsumer {
	return adp.consumer
}

func (adp *cmqPulsarAdapterImpl) Meta() CmqPulsarMetaServer {
	return adp.meta
}

func (adp *cmqPulsarAdapterImpl) Close() {
	adp.meta.Close()
	adp.consumer.Close()
	adp.producer.Close()
	adp.client.Close()
}
