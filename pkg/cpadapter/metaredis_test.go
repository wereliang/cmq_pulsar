package cpadapter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaRedis(t *testing.T) {
	opts := &AdapterOptions{
		MetaOpts: MetaOptions{
			Redis: RedisOptions{
				Address: "127.0.0.1:6379",
			},
		},
	}
	meta, err := newMetaServerRedis(*opts)
	assert.Nil(t, err)
	m, _ := meta.(*metaServerRedis)
	err = m.build()
	assert.Nil(t, err)
}
