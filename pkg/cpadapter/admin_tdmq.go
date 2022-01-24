package cpadapter

import (
	"fmt"
	"log"

	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	api "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tdmq/v20200217"
)

func NewTdmqAdmin(opts AdminOptions) (PulsarAdmin, error) {
	credential := common.NewCredential(opts.TdmqSecretID, opts.TdmqSecretKey)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = opts.AdminURL
	cli, _ := api.NewClient(credential, opts.TdmqRegion, cpf)
	return &tdmqAdmin{client: cli, opts: opts}, nil
}

type tdmqAdmin struct {
	client *api.Client
	opts   AdminOptions
}

func (adm *tdmqAdmin) ListTopics() ([]string, error) {
	return nil, nil
}

func (adm *tdmqAdmin) CreateTopic(name string) error {
	request := api.NewCreateTopicRequest()
	request.ClusterId = common.StringPtr(adm.opts.Cluster.Tenant)
	request.EnvironmentId = common.StringPtr(adm.opts.Cluster.Namespace)
	request.TopicName = common.StringPtr(name)
	request.Partitions = common.Uint64Ptr(2)
	request.TopicType = common.Uint64Ptr(2)
	response, err := adm.client.CreateTopic(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return fmt.Errorf("an api error has returned: %s", err)
	}
	if err != nil {
		return err
	}
	log.Printf("%s", response.ToJsonString())
	return nil
}

func (adm *tdmqAdmin) CreateSubscribe(topic string, subscribe string) error {
	request := api.NewCreateSubscriptionRequest()
	request.ClusterId = common.StringPtr(adm.opts.Cluster.Tenant)
	request.EnvironmentId = common.StringPtr(adm.opts.Cluster.Namespace)
	request.TopicName = common.StringPtr(topic)
	request.SubscriptionName = common.StringPtr(subscribe)
	request.IsIdempotent = common.BoolPtr(false)
	request.AutoCreatePolicyTopic = common.BoolPtr(false)
	response, err := adm.client.CreateSubscription(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		if err.(*errors.TencentCloudSDKError).GetCode() == "ResourceInUse.Subscription" {
			return ErrExist
		}
		return fmt.Errorf("an api error has returned: %s", err)
	}
	if err != nil {
		return err
	}
	log.Printf("%s", response.ToJsonString())
	return nil
}
func (adm *tdmqAdmin) GetSubscriptions(string) ([]string, error) {
	return nil, nil
}

func (adm *tdmqAdmin) Unsubscribe(topic string, subscribe string) error {
	request := api.NewDeleteSubscriptionsRequest()
	request.ClusterId = common.StringPtr(adm.opts.Cluster.Tenant)
	request.EnvironmentId = common.StringPtr(adm.opts.Cluster.Namespace)
	request.SubscriptionTopicSets = []*api.SubscriptionTopic{
		{
			EnvironmentId:    common.StringPtr(adm.opts.Cluster.Namespace),
			TopicName:        common.StringPtr(topic),
			SubscriptionName: common.StringPtr(topic),
		},
	}
	response, err := adm.client.DeleteSubscriptions(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return fmt.Errorf("an api error has returned: %s", err)
	}
	if err != nil {
		return err
	}
	log.Printf("%s", response.ToJsonString())
	return nil
}

func (adm *tdmqAdmin) Cluster() *ClusterInfo {
	return &adm.opts.Cluster
}
func (adm *tdmqAdmin) Close() {

}
