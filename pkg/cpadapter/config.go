package cpadapter

import plog "github.com/apache/pulsar-client-go/pulsar/log"

const (
	BackendPulsar = "pulsar"
	BackendTDMQ   = "tdmq"

	DefaultSchema    = "persistent"
	DefaultTenant    = "public"
	DefaultNamespace = "cmq"

	DefaultMetaInterval = 30 //second
	DefaultNackTimeout  = 60
)

type Config struct {
	Type    string         `yaml:"type"`
	Admin   AdminOptions   `yaml:"admin"`
	Meta    MetaOptions    `yaml:"meta"`
	Adapter AdapterOptions `yaml:"adapter"`
	Logger  plog.Logger
}

func must(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func (c *Config) Check() {
	typ := c.Type
	must(typ == BackendPulsar || typ == BackendTDMQ, "invalid backend type")

	// admin
	must(c.Admin.AdminURL != "", "empty admin url")
	must(c.Admin.Cluster.Tenant != "", "empty tenant")
	must(c.Admin.Cluster.Namespace != "", "empty namespace")
	must(c.Adapter.PulsarURL != "", "pulsar url empty")
	if typ == BackendPulsar {
		c.Admin.Cluster.Schema = DefaultSchema
	} else {
		c.Admin.Cluster.Schema = ""
	}
	if typ == BackendTDMQ {
		must(c.Admin.TdmqSecretID != "", "secretId empty")
		must(c.Admin.TdmqSecretKey != "", "secretKey empty")
		must(c.Admin.TdmqRegion != "", "region empty")
	}

	// meta
	must(c.Meta.Redis.Address != "", "redis address empty")
	if c.Meta.Interval <= 0 {
		c.Meta.Interval = DefaultMetaInterval
	}

	// adapter
	must(c.Adapter.PulsarURL != "", "pulsar url empty")
	if c.Adapter.NackTimeout <= 0 {
		c.Adapter.NackTimeout = DefaultNackTimeout
	}
	c.Adapter.Logger = c.Logger
}
