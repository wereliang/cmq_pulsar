package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/wereliang/cmq_pulsar/pkg/api"
)

type CmqContext interface {
	Writer() http.ResponseWriter
	Values() url.Values
	RequestID() string
	SetValues(url.Values)
}

type cmqContext struct {
	writer    http.ResponseWriter
	values    url.Values
	requestID string
}

func newCmqContexst(w http.ResponseWriter, values url.Values) CmqContext {
	return &cmqContext{
		writer:    w,
		values:    values,
		requestID: uuid.New().String(),
	}
}

func (ctx *cmqContext) Writer() http.ResponseWriter {
	return ctx.writer
}
func (ctx *cmqContext) Values() url.Values {
	return ctx.values
}
func (ctx *cmqContext) RequestID() string {
	return ctx.requestID
}
func (ctx *cmqContext) SetValues(v url.Values) {
	ctx.values = v
}

type CommHandler struct {
}

func (c *CommHandler) WriteError(ctx CmqContext, err error, v ...interface{}) {
	resp := &api.CommResponse{RequestId: ctx.RequestID()}
	if cerr, ok := err.(*CmqError); ok {
		resp.Code = cerr.ErrCode
		if len(v) != 0 {
			resp.Message = fmt.Sprintf(cerr.Error(), v...)
		} else {
			resp.Message = cerr.Error()
		}
	} else {
		resp.Code = 10000
		resp.Message = err.Error()
	}
	data, _ := json.Marshal(resp)
	log.Printf("response [%s] %s", ctx.RequestID(), string(data))
	ctx.Writer().Write(data)
}

func (c *CommHandler) WriteResp(ctx CmqContext, rsp api.CmqResponse) {
	rsp.SetRequestID(ctx.RequestID())
	data, _ := json.Marshal(rsp)
	log.Printf("response [%s] %s", ctx.RequestID(), string(data))
	ctx.Writer().Write(data)
}

func (c *CommHandler) Parse(ctx CmqContext, req interface{}) bool {
	err := builder.Parse(ctx.Values(), req)
	if err != nil {
		c.WriteError(ctx, ErrRequestParamError)
		return false
	}
	log.Printf("request [%s] %#v", ctx.RequestID(), req)
	return true
}
