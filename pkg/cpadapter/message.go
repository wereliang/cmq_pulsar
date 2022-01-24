package cpadapter

type Message interface {
	MsgBody() []byte
	MsgID() string
	ReceiptHandle() string
	DequeueCount() uint32
}

func newMessage(handle string, body []byte, msgId string, redeliveryCount uint32) Message {
	return &messageImpl{
		receipHandle: handle,
		body:         body,
		msgId:        msgId,
		dequeue:      redeliveryCount,
	}
}

type messageImpl struct {
	receipHandle string
	body         []byte
	dequeue      uint32
	msgId        string
}

func (msg *messageImpl) MsgBody() []byte {
	return msg.body
}

func (msg *messageImpl) MsgID() string {
	return msg.msgId
}

func (msg *messageImpl) ReceiptHandle() string {
	return msg.receipHandle
}

func (msg *messageImpl) DequeueCount() uint32 {
	return msg.dequeue
}
