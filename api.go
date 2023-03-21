package mqlib

import (
	"context"
	"time"
)

type RpcCallback = func(msg *Message)
type SubCallback = func(msg *Message) error

type SubDispatcher interface {
	ProcessMsg(msg *Message) error
}

type baseClient interface {
	Shutdown()
}

type PubClient interface {
	baseClient
	SetRpcTimeout(ttl time.Duration)
	SendMessage(msg *Message) error
	Request(msg *Message, ctx ...context.Context) (resp *Message, err error)
	RequestAsync(msg *Message, out chan *Message, ctx ...context.Context) error
	RequestAsyncWithFunc(msg *Message, callback RpcCallback, ctx ...context.Context) error
}

type SubClient interface {
	baseClient
}

type RpcSrvClient interface {
	baseClient
	Respond(msg *Message) error
}

type PubSubClient interface {
	PubClient
	SubClient
}

type GenericClient interface {
	RpcSrvClient
	PubSubClient
}
