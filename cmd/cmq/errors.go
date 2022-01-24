package main

import "fmt"

type CmqError struct {
	ErrCode int
	SubCode int
	Message string
}

func (e *CmqError) Error() string {
	return fmt.Sprintf("(%d)%s", e.SubCode, e.Message)
}

func newError(code, subCode int, message string) error {
	return &CmqError{code, subCode, message}
}

var (
	ErrInvalidParam        = newError(4000, 10000, "invalid request parameters")
	ErrLackRequestParam    = newError(4000, 10010, "lacked of required parameters")
	ErrOutOfRange          = newError(4000, 10350, "parameter %s value or length is out of range")
	ErrActionNotExist      = newError(4000, 10430, "action name %s is not existed")
	ErrRequestParamError   = newError(4000, 10110, "request parameters error")
	ErrUnExpectedMethod    = newError(4000, 10410, "unexpected http only POST is supported")
	ErrTopicNotExist       = newError(4000, 4440, "(10600)topic is not existed, or deleted")
	ErrQueueExist          = newError(4000, 4460, "queue is already existed,case insensitive")
	ErrInternal            = newError(6000, 10050, "server internal error")
	ErrQueueNoSubscription = newError(6030, 10651, "queue has no subscription, please create a subscription before receving message") // define myself
	ErrTopicNoSubscription = newError(6030, 10650, "topic has no subscription, please create a subscription before publishing message")
	ErrNoMessage           = newError(7000, 10200, "no message")
)
