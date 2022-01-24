package cpadapter

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	CMQTag = "CMQTAG"
)

var (
	ErrNotExist = errors.New("object not exist")
	ErrExist    = errors.New("object exist")
)

type ClusterInfo struct {
	Schema    string `yaml:"schema"`
	Tenant    string `yaml:"tenant"`
	Namespace string `yaml:"namespace"`
}

func MakeMessageID(msgID pulsar.MessageID) string {
	return fmt.Sprintf("%d:%d:%d",
		msgID.LedgerID(),
		msgID.EntryID(),
		msgID.PartitionIdx())
}

func MakePulsarTopic(cluster *ClusterInfo, topic string) string {
	if cluster.Schema == "" {
		return fmt.Sprintf("%s/%s/%s", cluster.Tenant, cluster.Namespace, topic)
	}
	return fmt.Sprintf("%s://%s/%s/%s", cluster.Schema, cluster.Tenant, cluster.Namespace, topic)
}

// tdmq topic: persistent://xxx/namespace/test001-partition-0
func MakeCMQTopic(topic string) string {
	ss := strings.Split(topic, "/")
	return strings.Split(ss[len(ss)-1], "-partition")[0]
}

type CMQObject interface {
	Check() bool
	IsNew(int64) bool
	UpdateVersion()
	ObjectName() string
}

type CommField struct {
	Name     string `json:"name"`
	UpdateTS int64  `json:"update"`
	Status   int    `json:"status"`
}

func (cf *CommField) Check() bool {
	if cf.Name == "" || cf.UpdateTS <= 0 {
		return false
	}
	return true
}

func (cf *CommField) IsNew(ts int64) bool {
	return cf.UpdateTS > ts
}

func (cf *CommField) UpdateVersion() {
	cf.UpdateTS = time.Now().UnixNano()
}

func (cf *CommField) ObjectName() string {
	return cf.Name
}

type CMQTopic struct {
	CommField
	Subscribes []*CMQSubscribe `json:"subscribes"`
}

type CMQQueue struct {
	CommField
	Subscribes []*CMQSubscribe `json:"subscribes"`
}

type CMQTags map[string]map[string]interface{}

func (q *CMQQueue) GetTopics() ([]string, CMQTags) {
	var res []string
	var tags CMQTags
	for _, s := range q.Subscribes {
		if s.Status == 0 {
			continue
		}
		res = append(res, s.Topic)
		if len(s.Tags) != 0 {
			if tags == nil {
				tags = make(CMQTags)
			}
			for _, tag := range s.Tags {
				if _, ok := tags[s.Topic]; !ok {
					tags[s.Topic] = make(map[string]interface{})
				}
				tags[s.Topic][tag] = true
			}
		}
	}
	return res, tags
}

type CMQSubscribe struct {
	CommField
	Topic string   `json:"topic"`
	Queue string   `json:"queue"`
	Tags  []string `json:"tags"`
}

type eventType int8

const (
	eventTopic eventType = iota
	eventQueue
)

type MetaEvent struct {
	Type      eventType
	Name      string
	Timestamp int64
	Cb        func()
}

type Status int8

const (
	StatusInit Status = iota
	StatusRunning
	StatusClosed
)

type ObjectActive interface {
	Active()
	LastActive() (time.Time, bool)
}

type commActive struct {
	last atomic.Value
}

func (act *commActive) Active() {
	act.last.Store(time.Now())
}

func (act *commActive) LastActive() (time.Time, bool) {
	if v, ok := act.last.Load().(time.Time); ok {
		return v, true
	} else {
		return time.Now(), false
	}
}
