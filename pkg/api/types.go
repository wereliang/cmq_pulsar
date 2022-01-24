package api

type CmqConfig struct {
	QueueURL        string
	TopicURL        string
	Region          string
	SecretId        string
	SecretKey       string
	SignatureMethod string
}

type CmqResponse interface {
	SetRequestID(string)
}

type CommResponse struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	RequestId string `json:"requestId"`
}

func (resp *CommResponse) SetRequestID(id string) {
	resp.RequestId = id
}

type CreateTopicRequest struct {
	TopicName  string `cmq:"topicName"`
	MaxMsgSize int    `cmq:"maxMsgSize"`
	FilterType int    `cmq:"filterType"`
}
type CreateTopicResponse struct {
	CommResponse
	TopicId string `json:"topicId"`
}

type ListTopicRequest struct {
	Offset int `cmq:"offset"`
	Limit  int `cmq:"limit"`
}
type TopicObject struct {
	TopicId   string `json:"topicId"`
	TopicName string `json:"topicName"`
}
type ListTopicResponse struct {
	CommResponse
	TotalCount int           `json:"totalCount"`
	TopicList  []TopicObject `json:"topicList"`
}

type ListSubscribeRequest struct {
	TopicName string `cmq:"topicName"`
	Offset    int    `cmq:"offset"`
	Limit     int    `cmq:"limit"`
}
type SubscribeObject struct {
	SubscriptionId   string `json:"subscriptionId"`
	SubscriptionName string `json:"subscriptionName"`
	Protocol         string `json:"protocol"`
	Endpoint         string `json:"endpoint"`
}
type ListSubscribeResponse struct {
	CommResponse
	TotalCount       int               `json:"totalCount"`
	SubscriptionList []SubscribeObject `json:"subscriptionList"`
}

type ListQueueRequest struct {
	Offset int `cmq:"offset"`
	Limit  int `cmq:"limit"`
}
type QueueObject struct {
	QueueId   string `json:"queueId"`
	QueueName string `json:"queueName"`
}
type ListQueueResponse struct {
	CommResponse
	TotalCount int           `json:"totalCount"`
	QueueList  []QueueObject `json:"queueList"`
}

type CreateQueueRequest struct {
	QueueName           string `cmq:"queueName"`
	MaxMsgHeapNum       int    `cmq:"maxMsgHeapNum"`
	PollingWaitSeconds  int    `cmq:"pollingWaitSeconds"`
	VisibilityTimeout   int    `cmq:"visibilityTimeout"`
	MaxMsgSize          int    `cmq:"maxMsgSize"`
	MsgRetentionSeconds int    `cmq:"msgRetentionSeconds"`
	RewindSeconds       int    `cmq:"rewindSeconds"`
}
type CreateQueueResponse struct {
	CommResponse
	QueueId string `json:"queueId"`
}

type CreateSubscribeRequest struct {
	TopicName           string `cmq:"topicName"`
	SubscriptionName    string `cmq:"subscriptionName"`
	Protocol            string `cmq:"protocol"`
	Endpoint            string `cmq:"endpoint"`
	NotifyStrategy      string `cmq:"notifyStrategy"`
	NotifyContentFormat string `cmq:"notifyContentFormat"`
	FilterTag0          string `cmq:"filterTag.0"`
	FilterTag1          string `cmq:"filterTag.1"`
	FilterTag2          string `cmq:"filterTag.2"`
	FilterTag3          string `cmq:"filterTag.3"`
	FilterTag4          string `cmq:"filterTag.4"`
}
type CreateSubscribeResponse struct {
	CommResponse
	CodeDesc string `cmq:"codeDesc"`
}

type UnsubscribeRequest struct {
	TopicName        string `cmq:"topicName"`
	SubscriptionName string `cmq:"subscriptionName"`
}
type UnsubscribeResponse struct {
	CommResponse
}

type MessageResponse struct {
	MsgBody          string `json:"msgBody"`
	MsgId            string `json:"msgId"`
	ReceiptHandle    string `json:"receiptHandle"`
	EnqueueTime      int    `json:"enqueueTime"`
	NextVisibleTime  int    `json:"nextVisibleTime"`
	DequeueCount     int    `json:"dequeueCount"`
	FirstDequeueTime int    `json:"firstDequeueTime"`
}

type ReceiveMessageRequest struct {
	QueueName          string `cmq:"queueName"`
	PollingWaitSeconds int    `cmq:"pollingWaitSeconds"`
}
type ReceiveMessageResponse struct {
	CommResponse
	MessageResponse
}

type BatchReceiveMessageRequest struct {
	QueueName          string `cmq:"queueName"`
	NumOfMsg           int    `cmq:"numOfMsg"`
	PollingWaitSeconds int    `cmq:"pollingWaitSeconds"`
}
type BatchReceiveMessageResponse struct {
	CommResponse
	MsgInfoList []MessageResponse `json:"msgInfoList"`
}

type DeleteMessageRequest struct {
	QueueName     string `cmq:"queueName"`
	ReceiptHandle string `cmq:"receiptHandle"`
}
type DeleteMessageResponse struct {
	CommResponse
}

type PublishMessageRequest struct {
	TopicName string `cmq:"topicName"`
	MsgBody   string `cmq:"msgBody"`
	MsgTag0   string `cmq:"msgTag.0"`
	MsgTag1   string `cmq:"msgTag.1"`
	MsgTag2   string `cmq:"msgTag.2"`
	MsgTag3   string `cmq:"msgTag.3"`
	MsgTag4   string `cmq:"msgTag.4"`
}
type PublishMessageResponse struct {
	CommResponse
	MsgId string `json:"msgId"`
}

type BatchPublishMessageRequest struct {
	TopicName string   `cmq:"topicName"`
	MsgBody   []string `cmq:"msgBody.0-15"`
}
type BatchPublishMessageResponse struct {
	CommResponse
	MsgList []struct {
		MsgId string `json:"msgId"`
	} `json:"msgList"`
}
