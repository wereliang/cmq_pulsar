package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type cmqServer struct {
	CommHandler
	addr      string
	actions   map[string]func(CmqContext)
	queue     *QueueHandler
	topic     *TopicHandler
	subscribe *SubscribeHandler
}

func newCmqServer(addr string) (*cmqServer, error) {
	server := &cmqServer{
		addr:      addr,
		actions:   make(map[string]func(CmqContext)),
		queue:     &QueueHandler{},
		topic:     &TopicHandler{},
		subscribe: &SubscribeHandler{},
	}
	server.route(http.DefaultServeMux)
	return server, nil
}

func (s *cmqServer) route(mux *http.ServeMux) {
	s.actions = map[string]func(ctx CmqContext){
		"CreateTopic":             s.topic.CreateTopic,
		"CreateQueue":             s.queue.CreateQueue,
		"Subscribe":               s.subscribe.CreateSubscribe,
		"Unsubscribe":             s.subscribe.Unsubscribe,
		"ListQueue":               s.queue.ListQueue,
		"ListTopic":               s.topic.ListTopic,
		"ListSubscriptionByTopic": s.subscribe.ListSubscribeByTopic,
		"ReceiveMessage":          s.queue.ReceiveMessage,
		"BatchReceiveMessage":     s.queue.BatchReceiveMessage,
		"DeleteMessage":           s.queue.DeleteMessage,
		"PublishMessage":          s.topic.PublishMessage,
		"BatchPublishMessage":     s.topic.BatchPublishMessage,
	}

	http.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/v2/index.php", func(w http.ResponseWriter, r *http.Request) {
		ctx := newCmqContexst(w, nil)

		if r.Method != http.MethodPost {
			s.WriteError(ctx, ErrUnExpectedMethod)
			return
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			s.WriteError(ctx, ErrInternal)
			return
		}
		log.Println(string(body))

		values, err := url.ParseQuery(string(body))
		if err != nil {
			s.WriteError(ctx, ErrInvalidParam)
			return
		}
		ctx.SetValues(values)

		action := values.Get("Action")
		if action == "" {
			s.WriteError(ctx, ErrLackRequestParam)
			return
		}

		if handler, ok := s.actions[action]; !ok {
			s.WriteError(ctx, ErrActionNotExist, action)
			return
		} else {
			handler(ctx)
		}
	})
}

func (s *cmqServer) start() {
	http.ListenAndServe(s.addr, nil)
}
