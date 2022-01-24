// 为求简单，这里假设对meta的更新不是很频繁，对meta的一致性要求也不高
// 1. redis中topic, subscribe, queue 三张表，topic和queue存属性，subscribe存关系
// 2. 内存topic和queue中包含了subscribe，都是从subscribe构建过来。内存中包含了最新的update时间戳
// 3. subscribe更新之后会同步更新topic和subscribe并触发事件；topic和queue本身的更新也会触发事件
// 4. 创建事件先写pulsar，再更新redis，本地内存统一定时更新
// 5. 读操作通过内存，写操作通过redis
// 6. build期间加锁，不接受新写入。通过unixnano一定程度避免更新丢失
package cpadapter

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	TopicHashKey     = "cmq_topic"
	QueueHashKey     = "cmq_queue"
	SubscribeHashKey = "cmq_subscribe"
)

type RedisOptions struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type MetaOptions struct {
	Redis    RedisOptions `yaml:"redis"`
	Interval int64        `yaml:"interval"`
}

type CmqPulsarMetaServer interface {
	CreateTopic(string) error
	QueryTopic(string) *CMQTopic
	ListTopic() ([]*CMQTopic, error)

	CreateQueue(string) error
	QueryQueue(string) *CMQQueue
	ListQueue() ([]*CMQQueue, error)

	CreateSubscribe(string, string, string, []string) error
	QuerySubscribe(string) *CMQSubscribe
	ListSubscribe() ([]*CMQSubscribe, error)
	DeleteSubscribe(string) error

	AddWatch(ch chan<- *MetaEvent)
	Admin() PulsarAdmin
	Close()
}

type metaServerRedis struct {
	opts  MetaOptions
	admin PulsarAdmin
	pool  *redis.Pool
	rds   *redisWrap
	wcs   []chan<- *MetaEvent

	sync.RWMutex
	verCtl          sync.RWMutex // 版本控制，更新和build互斥
	topics          map[string]*CMQTopic
	queues          map[string]*CMQQueue
	subscribes      map[string]*CMQSubscribe
	topicUpdate     int64
	queueUpdate     int64
	subscribeUpdate int64
}

func NewMetaServerRedis(admin PulsarAdmin, opts MetaOptions) (CmqPulsarMetaServer, error) {
	pool := &redis.Pool{
		MaxIdle:     20,
		MaxActive:   10,
		IdleTimeout: 30 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", opts.Redis.Address)
			if err != nil {
				return nil, err
			}
			if len(opts.Redis.Password) > 0 {
				if _, err = c.Do("AUTH", opts.Redis.Password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if len(opts.Redis.Database) > 0 {
				if _, err = c.Do("SELECT", opts.Redis.Database); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
		},
	}

	if opts.Interval%5 != 0 {
		opts.Interval = (opts.Interval/5 + 1) * 5
	}

	ms := &metaServerRedis{
		opts:       opts,
		admin:      admin,
		pool:       pool,
		rds:        &redisWrap{},
		topics:     make(map[string]*CMQTopic),
		queues:     make(map[string]*CMQQueue),
		subscribes: make(map[string]*CMQSubscribe),
	}
	ms.build()
	go ms.task()
	return ms, nil
}

func (ms *metaServerRedis) Close() {
	ms.pool.Close()
}

func (ms *metaServerRedis) Admin() PulsarAdmin {
	return ms.admin
}

func (ms *metaServerRedis) AddWatch(ch chan<- *MetaEvent) {
	ms.wcs = append(ms.wcs, ch)
}

func (ms *metaServerRedis) setObject(c redis.Conn, obj CMQObject) error {
	ms.verCtl.RLock()
	defer ms.verCtl.RUnlock()
	obj.UpdateVersion()
	switch obj.(type) {
	case *CMQTopic:
		return ms.rds.setTopic(c, obj)
	case *CMQQueue:
		return ms.rds.setQueue(c, obj)
	case *CMQSubscribe:
		return ms.rds.setSubscribe(c, obj)
	default:
		panic("unknow cmq object")
	}
}

func (ms *metaServerRedis) CreateTopic(name string) error {
	// Query from redis
	c := ms.pool.Get()
	defer c.Close()
	_, err := ms.rds.queryTopic(c, name)
	if err != ErrNotExist {
		if err != nil {
			return fmt.Errorf("query topic error. %s", err)
		} else {
			return ErrExist
		}
	}

	// Create to pulsar. Continue if exist
	err = ms.admin.CreateTopic(name)
	if err != nil {
		if err != ErrExist {
			return fmt.Errorf("create topic to pulsar error. %s", err)
		}
	}

	// Create to redis
	return ms.setObject(c, &CMQTopic{
		CommField: CommField{
			Name:   name,
			Status: 1,
		},
	})
}

func (ms *metaServerRedis) ListTopic() ([]*CMQTopic, error) {
	var res []*CMQTopic
	ms.RLock()
	defer ms.RUnlock()
	for _, v := range ms.topics {
		if v.Status != 0 {
			res = append(res, v)
		}
	}
	return res, nil
}

func (ms *metaServerRedis) QueryTopic(name string) *CMQTopic {
	ms.RLock()
	defer ms.RUnlock()
	if topic, ok := ms.topics[name]; ok {
		return topic
	}
	return nil
}

func (ms *metaServerRedis) CreateQueue(name string) error {
	// Query from redis
	c := ms.pool.Get()
	defer c.Close()
	que, err := ms.rds.queryQueue(c, name)
	if err != ErrNotExist {
		if err != nil {
			return fmt.Errorf("query queue error. %s", err)
		} else if que.Status == 1 {
			return ErrExist
		}
	}

	// nothing for pulsar

	// create to redis
	return ms.setObject(c, &CMQQueue{
		CommField: CommField{
			Name:   name,
			Status: 1,
		},
	})
}

func (ms *metaServerRedis) QueryQueue(name string) *CMQQueue {
	ms.RLock()
	defer ms.RUnlock()
	if queue, ok := ms.queues[name]; ok {
		return queue
	}
	return nil
}

func (ms *metaServerRedis) ListQueue() ([]*CMQQueue, error) {
	var res []*CMQQueue
	ms.RLock()
	defer ms.RUnlock()
	for _, v := range ms.queues {
		res = append(res, v)
	}
	return res, nil
}

func (ms *metaServerRedis) CreateSubscribe(name, queue, topic string, tags []string) error {
	// Query from redis
	c := ms.pool.Get()
	defer c.Close()
	sub, err := ms.rds.querySubscribe(c, name)
	if err != ErrNotExist {
		if err != nil {
			return fmt.Errorf("query subscribe error. %s", err)
		} else if sub.Status == 1 {
			return fmt.Errorf("subscribe exist")
		}
	}

	_, err = ms.rds.queryQueue(c, queue)
	if err != nil {
		if err == ErrNotExist {
			return fmt.Errorf("queue %s not exist", queue)
		} else {
			return fmt.Errorf("query queue %s error. %s", queue, err)
		}
	}

	_, err = ms.rds.queryTopic(c, topic)
	if err != nil {
		if err == ErrNotExist {
			return fmt.Errorf("topic %s not exist", queue)
		} else {
			return fmt.Errorf("query topic %s error. %s", queue, err)
		}
	}

	// create to pulsar
	err = ms.admin.CreateSubscribe(topic, queue)
	if err != nil {
		if err != ErrExist {
			return fmt.Errorf("create subscribe to pulsar error. %s", err)
		}
	}

	//
	return ms.setObject(c, &CMQSubscribe{
		CommField: CommField{
			Name:   name,
			Status: 1,
		},
		Queue: queue,
		Topic: topic,
		Tags:  tags,
	})
}

func (ms *metaServerRedis) DeleteSubscribe(name string) error {
	// Query from redis
	c := ms.pool.Get()
	defer c.Close()
	sub, err := ms.rds.querySubscribe(c, name)
	if err != nil {
		return err
	}
	// unsubscribe pulsar by event callback
	// reset redis
	sub.Status = 0
	return ms.setObject(c, sub)
}

func (ms *metaServerRedis) QuerySubscribe(name string) *CMQSubscribe {
	ms.RLock()
	defer ms.RUnlock()
	if sub, ok := ms.subscribes[name]; ok {
		return sub
	}
	return nil
}

func (ms *metaServerRedis) ListSubscribe() ([]*CMQSubscribe, error) {
	var res []*CMQSubscribe
	ms.RLock()
	defer ms.RUnlock()
	for _, v := range ms.subscribes {
		res = append(res, v)
	}
	return res, nil
}

func (ms *metaServerRedis) calcNext() time.Duration {
	interval := ms.opts.Interval
	now := time.Now().Unix()
	next := (now/interval + 1) * interval
	timeout := next - now
	if next-now < 5 {
		timeout += interval
	}
	return time.Duration(timeout) * time.Second
}

func (ms *metaServerRedis) task() {
	// t := time.NewTicker(time.Second * time.Duration(ms.opts.Interval))
	t := time.NewTimer(ms.calcNext())
	for {
		select {
		case <-t.C:
			t.Reset(ms.calcNext())
			err := ms.build()
			if err != nil {
				log.Println("build fail.", err)
			}
		}
	}
}

func (ms *metaServerRedis) updateTopics(topics map[string]*CMQTopic) {
	log.Printf("update topic: %#v", topics)

	latest := ms.topicUpdate
	for k, v := range topics {
		if v.UpdateTS > latest {
			latest = v.UpdateTS
		}
		if t, ok := ms.topics[k]; ok {
			if v.Status == 0 {
				delete(ms.topics, k)
			} else {
				v.Subscribes = t.Subscribes
				ms.topics[k] = v
			}
		} else {
			ms.topics[k] = v
		}
	}
	ms.topicUpdate = latest
}

func (ms *metaServerRedis) updateQueues(queues map[string]*CMQQueue) {
	log.Printf("update queue: %#v", queues)

	latest := ms.queueUpdate
	for k, v := range queues {
		if v.UpdateTS > latest {
			latest = v.UpdateTS
		}
		if q, ok := ms.queues[k]; ok {
			if v.Status == 0 {
				delete(ms.queues, k)
			} else {
				v.Subscribes = q.Subscribes
				ms.queues[k] = v
			}
		} else {
			ms.queues[k] = v
		}
	}
	ms.queueUpdate = latest
}

func (ms *metaServerRedis) updateSubscribes(subs map[string]*CMQSubscribe) {
	log.Printf("update subscribe: %#v", subs)

	latest := ms.subscribeUpdate
	for k, v := range subs {
		if v.UpdateTS > latest {
			latest = v.UpdateTS
		}
		if sub, ok := ms.subscribes[k]; ok {
			// logic delete
			if v.Status == 0 {
				if t, o := ms.topics[v.Topic]; o {
					var newSubs []*CMQSubscribe
					for _, s := range t.Subscribes {
						if s != sub {
							newSubs = append(newSubs, s)
						}
					}
					t.Subscribes = newSubs
				}
				if q, o := ms.queues[v.Queue]; o {
					var newSubs []*CMQSubscribe
					for _, s := range q.Subscribes {
						if s != sub {
							newSubs = append(newSubs, s)
						}
					}
					q.Subscribes = newSubs
				}
				delete(ms.subscribes, k)
			} else { // update
				ms.subscribes[k] = v
			}
		} else {
			if v.Status == 0 {
				continue
			}
			// create
			if t, o := ms.topics[v.Topic]; !o {
				panic(fmt.Errorf("topic not exist for subscribe:(%#v)", v))
			} else {
				t.Subscribes = append(t.Subscribes, v)
			}
			if q, o := ms.queues[v.Queue]; !o {
				panic(fmt.Errorf("queue not exist for subscribe:(%#v)", v))
			} else {
				q.Subscribes = append(q.Subscribes, v)
			}
			ms.subscribes[k] = v
		}
	}
	ms.subscribeUpdate = latest
}

func (ms *metaServerRedis) build() error {
	log.Printf("build topic[%d] queue[%d] subscribe[%d]",
		ms.topicUpdate, ms.queueUpdate, ms.subscribeUpdate)

	c := ms.pool.Get()
	defer c.Close()

	var (
		topics     map[string]*CMQTopic
		queues     map[string]*CMQQueue
		subscribes map[string]*CMQSubscribe
		err        error
	)

	func() {
		ms.verCtl.Lock()
		defer ms.verCtl.Unlock()
		topics, err = ms.rds.getUpdateTopics(c, ms.topicUpdate)
		if err != nil {
			return
		}
		queues, err = ms.rds.getUpdateQueues(c, ms.queueUpdate)
		if err != nil {
			return
		}
		subscribes, err = ms.rds.getUpdateSubscribes(c, ms.subscribeUpdate)
		if err != nil {
			return
		}
	}()
	if err != nil {
		return err
	}

	ms.Lock()
	ms.updateTopics(topics)
	ms.updateQueues(queues)
	ms.updateSubscribes(subscribes)
	ms.Unlock()

	ms.notify(topics, queues, subscribes)
	return nil
}

func (ms *metaServerRedis) notifyEvent(e *MetaEvent) {
	for _, c := range ms.wcs {
		c <- e
	}
}

func (ms *metaServerRedis) notify(
	topics map[string]*CMQTopic,
	queues map[string]*CMQQueue,
	subscribes map[string]*CMQSubscribe) {

	if len(ms.wcs) == 0 {
		return
	}

	for k, t := range topics {
		ms.notifyEvent(&MetaEvent{
			Type:      eventTopic,
			Name:      k,
			Timestamp: t.UpdateTS,
		})
	}

	for k, q := range queues {
		ms.notifyEvent(&MetaEvent{
			Type:      eventQueue,
			Name:      k,
			Timestamp: q.UpdateTS,
		})
	}

	for _, s := range subscribes {
		if _, ok := topics[s.Topic]; !ok {
			ms.notifyEvent(&MetaEvent{
				Type:      eventTopic,
				Name:      s.Topic,
				Timestamp: s.UpdateTS,
			})
		}

		if _, ok := queues[s.Queue]; !ok {
			var fn func()
			if s.Status == 0 {
				fn = func() {
					// unsubscribe pulsar
					err := ms.admin.Unsubscribe(s.Topic, s.Queue)
					if err != nil {
						log.Printf("unsubscribe from pulsar error. %s %s", s.Topic, s.Queue)
					} else {
						log.Printf("unsubscribe from pulsar ok. %s %s", s.Topic, s.Queue)
					}
				}
			}

			ms.notifyEvent(&MetaEvent{
				Type:      eventQueue,
				Name:      s.Queue,
				Timestamp: s.UpdateTS,
				Cb:        fn,
			})
		}
	}
}

type redisWrap struct {
}

func (r *redisWrap) query(c redis.Conn, key, field string, obj interface{}) error {
	res, err := redis.String(c.Do("HGET", key, field))
	if err != nil {
		if err == redis.ErrNil {
			return ErrNotExist
		}
		return err
	}
	err = json.Unmarshal([]byte(res), obj)
	if err != nil {
		return err
	}
	return nil
}

func (r *redisWrap) set(c redis.Conn, key, field string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = c.Do("HSET", key, field, string(data))
	if err != nil {
		return err
	}
	return nil
}

func (r *redisWrap) getAll(c redis.Conn, key string) (map[string]string, error) {
	res, err := redis.StringMap(c.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (r *redisWrap) getUpdateTopics(c redis.Conn, ts int64) (map[string]*CMQTopic, error) {
	res, err := r.getAll(c, TopicHashKey)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*CMQTopic)
	for k, v := range res {
		topic := &CMQTopic{}
		err = json.Unmarshal([]byte(v), topic)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal fail:%s. invalid topic data:%s", err, v)
		}
		if !topic.Check() {
			log.Printf("invalid object:%#v", topic)
			continue
		}
		if topic.UpdateTS > ts {
			result[k] = topic
		}
	}
	return result, nil
}

func (r *redisWrap) getUpdateSubscribes(c redis.Conn, ts int64) (map[string]*CMQSubscribe, error) {
	res, err := r.getAll(c, SubscribeHashKey)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*CMQSubscribe)
	for k, v := range res {
		subscribe := &CMQSubscribe{}
		err = json.Unmarshal([]byte(v), subscribe)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal fail:%s. invalid subscribe data:%s", err, v)
		}
		if subscribe.UpdateTS > ts {
			result[k] = subscribe
		}
	}
	return result, nil
}

func (r *redisWrap) getUpdateQueues(c redis.Conn, ts int64) (map[string]*CMQQueue, error) {
	res, err := r.getAll(c, QueueHashKey)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*CMQQueue)
	for k, v := range res {
		queue := &CMQQueue{}
		err = json.Unmarshal([]byte(v), queue)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal fail:%s. invalid queue data:%s", err, v)
		}
		if queue.UpdateTS > ts {
			result[k] = queue
		}
	}
	return result, nil
}

func (r *redisWrap) queryTopic(c redis.Conn, name string) (*CMQTopic, error) {
	topic := &CMQTopic{}
	if err := r.query(c, TopicHashKey, name, topic); err != nil {
		return nil, err
	}
	return topic, nil
}

func (r *redisWrap) queryQueue(c redis.Conn, name string) (*CMQQueue, error) {
	queue := &CMQQueue{}
	if err := r.query(c, QueueHashKey, name, queue); err != nil {
		return nil, err
	}
	return queue, nil
}

func (r *redisWrap) querySubscribe(c redis.Conn, name string) (*CMQSubscribe, error) {
	subscribe := &CMQSubscribe{}
	if err := r.query(c, SubscribeHashKey, name, subscribe); err != nil {
		return nil, err
	}
	return subscribe, nil
}

func (r *redisWrap) setTopic(c redis.Conn, t CMQObject) error {
	return r.set(c, TopicHashKey, t.ObjectName(), t)
}

func (r *redisWrap) setQueue(c redis.Conn, q CMQObject) error {
	return r.set(c, QueueHashKey, q.ObjectName(), q)
}

func (r *redisWrap) setSubscribe(c redis.Conn, s CMQObject) error {
	return r.set(c, SubscribeHashKey, s.ObjectName(), s)
}
