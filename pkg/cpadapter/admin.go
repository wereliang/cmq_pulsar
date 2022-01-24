package cpadapter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

const (
	AdminV2 = "admin/v2"
)

type AdminOptions struct {
	Cluster       ClusterInfo `yaml:"cluster"`
	AdminURL      string      `yaml:"adminUrl"`
	TdmqSecretID  string      `yaml:"tdmqSecretId"`
	TdmqSecretKey string      `yaml:"tdmqSecretKey"`
	TdmqRegion    string      `yaml:"tdmqRegion"`
}

type PulsarAdmin interface {
	ListTopics() ([]string, error)
	CreateTopic(string) error
	CreateSubscribe(string, string) error
	GetSubscriptions(string) ([]string, error)
	Unsubscribe(string, string) error
	Cluster() *ClusterInfo
	Close()
}

func NewPulsarAdmin(opts AdminOptions) (PulsarAdmin, error) {
	return &pulsarAdmin{
		opts: opts,
		rest: &restApi{
			adminURL:  opts.AdminURL,
			schema:    opts.Cluster.Schema,
			tenant:    opts.Cluster.Tenant,
			namespace: opts.Cluster.Namespace,
		},
	}, nil
}

var transport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 60 * time.Second,
		DualStack: true,
	}).DialContext,
	MaxIdleConns:          800,
	MaxIdleConnsPerHost:   400,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 0 * time.Second,
}

type restApi struct {
	adminURL  string
	schema    string
	tenant    string
	namespace string
}

func (api *restApi) nameSpace() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s",
		api.adminURL,
		AdminV2,
		api.schema,
		api.tenant,
		api.namespace)
}

func (api *restApi) listTopics() string {
	return api.nameSpace()
}

func (api *restApi) createTopic(topic string) string {
	return api.nameSpace() + "/" + topic
}

// GET /admin/v2/:schema/:tenant/:namespace/:topic/subscriptions
func (api *restApi) subscriptions(topic string) string {
	return fmt.Sprintf("%s/%s/subscriptions", api.nameSpace(), topic)
}

// PUT /admin/v2/persistent/:tenant/:namespace/:topic/subscription/:subscription
func (api *restApi) createSubscription(topic, subscribe string) string {
	return fmt.Sprintf("%s/%s/subscription/%s", api.nameSpace(), topic, subscribe)
}

// DELETE /admin/v2/persistent/:tenant/:namespace/:topic/subscription/:subscription
func (api *restApi) unSubscribe(topic, subscribe string) string {
	return fmt.Sprintf("%s/%s/subscription/%s", api.nameSpace(), topic, subscribe)
}

type pulsarAdmin struct {
	opts AdminOptions
	rest *restApi
}

func (adm *pulsarAdmin) Close() {

}

func (adm *pulsarAdmin) Cluster() *ClusterInfo {
	return &adm.opts.Cluster
}

func (adm *pulsarAdmin) httpGet(url string) ([]byte, error) {
	client := &http.Client{Transport: transport, Timeout: time.Second * 3}
	req, _ := http.NewRequest("GET", url, nil)
	rsp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer rsp.Body.Close()
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusNoContent {
		if rsp.StatusCode == http.StatusConflict {
			err = ErrExist
		} else {
			err = fmt.Errorf("%s http error. status:%d", string(data), rsp.StatusCode)
		}
	}
	return data, err
}

func (adm *pulsarAdmin) httpPut(url string) ([]byte, error) {
	client := &http.Client{Transport: transport, Timeout: time.Second * 3}
	req, _ := http.NewRequest("PUT", url, nil)
	rsp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer rsp.Body.Close()
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusNoContent {
		if rsp.StatusCode == http.StatusConflict {
			err = ErrExist
		} else {
			err = fmt.Errorf("%s http error. status:%d", string(data), rsp.StatusCode)
		}
	}
	return data, err
}

func (adm *pulsarAdmin) httpDelete(url string) ([]byte, error) {
	client := &http.Client{Transport: transport, Timeout: time.Second * 3}
	req, _ := http.NewRequest("DELETE", url, nil)
	rsp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer rsp.Body.Close()
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusNoContent {
		if rsp.StatusCode == http.StatusConflict {
			err = ErrExist
		} else {
			err = fmt.Errorf("%s http error. status:%d", string(data), rsp.StatusCode)
		}
	}
	return data, err
}

func (adm *pulsarAdmin) ListTopics() ([]string, error) {
	rest := adm.rest.listTopics()
	data, err := adm.httpGet(rest)
	if err != nil {
		return nil, err
	}

	var result []string
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (adm *pulsarAdmin) CreateTopic(topic string) error {
	rest := adm.rest.createTopic(topic)
	_, err := adm.httpPut(rest)
	return err
}

func (adm *pulsarAdmin) GetSubscriptions(topic string) ([]string, error) {
	rest := adm.rest.subscriptions(topic)
	data, err := adm.httpGet(rest)
	if err != nil {
		return nil, err
	}

	var result []string
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (adm *pulsarAdmin) CreateSubscribe(topic, subscribe string) error {
	rest := adm.rest.createSubscription(topic, subscribe)
	_, err := adm.httpPut(rest)
	return err
}

func (adm *pulsarAdmin) Unsubscribe(topic, subscribe string) error {
	rest := adm.rest.unSubscribe(topic, subscribe)
	data, err := adm.httpDelete(rest)
	if err != nil {
		return fmt.Errorf("unsubscribe fail. (%s) %s", string(data), err)
	}
	return nil
}
