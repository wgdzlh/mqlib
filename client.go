package mqlib

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/wgdzlh/mqlib/log"

	"github.com/wgdzlh/mqlib/rk/consumer"
	"github.com/wgdzlh/mqlib/rk/primitive"
	"github.com/wgdzlh/mqlib/rk/rlog"
	"go.uber.org/zap"
)

const (
	pingTopic      = "ping-msg"
	pubGpSuffix    = "-pub-gp"
	subGpSuffix    = "-sub-gp"
	rpcTopicSuffix = "-rpc"
	DefaultRpcTTL  = time.Minute * 10 // 默认rpc请求timeout
	sendRetry      = 3
	retryInterval  = time.Millisecond * 200
	// rpcGpSep       = "%"
	// tagKeySep      = "@"
)

var (
	ErrSendFailed   = errors.New("failed to send msg")
	ErrFetchFailed  = errors.New("failed to fetch msg")
	ErrMisformedMsg = errors.New("input msg is invalid")
	ErrMsgTimeout   = errors.New("msg timeout")
	ErrReachedLimit = errors.New("reached concurrent limit")
)

type client struct {
	Name       string        // MQ客户端名称（即本服务名称，或自定义的ConsumerGroup）
	nameServer string        // MQ的NameServer地址
	pub        *Producer     // MQ客户端公用的生产者
	sub        *Consumer     // 可接收RPC调用的MQ客户端消费者
	rpcTTL     time.Duration // 客户端发出的RPC请求的timeout
}

func init() { // 设置rocketMQ客户端日志路径在本可执行文件同目录下
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	rkLogPath := filepath.Join(filepath.Dir(ex), "rocketmq.log")
	rlog.SetLogLevel("warn")
	rlog.SetOutputPath(rkLogPath)
	log.Info("rocketmq log path", zap.String("rkLog", rkLogPath))
}

// 只接收RPC调用的MQ客户端
func NewSrvClient(nameServer, app string, subDispatcher SubDispatcher) (rsc RpcSrvClient, err error) {
	c, err := newClient(nameServer, app)
	if err != nil {
		return
	}
	apiSubTopic := c.getSrvSubTopic(subDispatcher)
	c.sub, err = NewConsumer(app+subGpSuffix, nameServer, false, false, apiSubTopic)
	rsc = c
	return
}

// 只做RPC调用/发布消息的MQ客户端
func NewClient(nameServer, app string, ttl ...time.Duration) (pc PubClient, err error) {
	return newClient(nameServer, app, ttl...)
}

// 只订阅消息的MQ客户端
func NewSubClient(nameServer, consumerGroup string, topics ...Topic) (sc SubClient, err error) {
	c := &client{
		Name:       consumerGroup,
		nameServer: nameServer,
	}
	c.sub, err = NewConsumer(consumerGroup, nameServer, true, false, topics...)
	return c, err
}

// 以广播模式订阅消息的MQ客户端
func NewBroadcastSubClient(nameServer, consumerGroup string, topics ...Topic) (sc SubClient, err error) {
	c := &client{
		Name:       consumerGroup,
		nameServer: nameServer,
	}
	c.sub, err = NewConsumer(consumerGroup, nameServer, false, true, topics...)
	return c, err
}

// 可发布/订阅消息的MQ客户端
func NewPubSubClient(nameServer, consumerGroup string, topics ...Topic) (psc PubSubClient, err error) {
	c, err := newClient(nameServer, consumerGroup)
	if err != nil {
		return
	}
	c.sub, err = NewConsumer(consumerGroup, nameServer, true, false, topics...)
	psc = c
	return
}

// 可发布/订阅消息，同时接收RPC调用的MQ客户端
func NewGenericClient(nameServer, app string, subDispatcher SubDispatcher, ttl time.Duration, topics ...Topic) (gc GenericClient, err error) {
	c, err := newClient(nameServer, app, ttl)
	if err != nil {
		return
	}
	topics = append(topics, c.getSrvSubTopic(subDispatcher))
	c.sub, err = NewConsumer(app+subGpSuffix, nameServer, false, false, topics...)
	gc = c
	return
}

func newClient(nameServer, app string, ttl ...time.Duration) (c *client, err error) {
	c = &client{
		Name:       app,
		nameServer: nameServer,
	}
	if len(ttl) > 0 && ttl[0] > 0 {
		c.rpcTTL = ttl[0]
	} else {
		c.rpcTTL = DefaultRpcTTL
	}
	c.pub, err = NewProducer(app+pubGpSuffix, nameServer)
	if err != nil {
		return
	}
	err = c.SendMessage(&Message{
		Topic: pingTopic,
		Tag:   app,
		Keys:  []string{strconv.FormatInt(time.Now().Unix(), 10)},
	})
	return
}

func (c *client) SetRpcTimeout(ttl time.Duration) {
	if ttl > 0 {
		c.rpcTTL = ttl
	}
}

func (c *client) Shutdown() {
	if c.pub != nil {
		c.pub.Shutdown()
	}
	if c.sub != nil {
		c.sub.Shutdown()
	}
}

func (c *client) getSrvSubTopic(dispatcher SubDispatcher) Topic {
	return Topic{
		Name:     c.Name + rpcTopicSuffix,
		Filter:   consumer.MessageSelector{},
		Callback: dispatcher.ProcessMsg,
	}
}

func (c *client) checkReqMsg(msg *Message) (err error) {
	if msg.RemoteApp == "" || msg.Tag == "" || msg.Topic != "" {
		err = ErrMisformedMsg
	}
	msg.Topic = msg.RemoteApp + rpcTopicSuffix
	return
}

func (c *client) checkRespMsg(msg *Message) (err error) {
	if msg.rawMsg == nil {
		err = ErrMisformedMsg
	}
	return
}

// 发布消息（同步阻塞）
func (c *client) SendMessage(msg *Message) (err error) {
	return c.sendRawMsg(msg.ToRkMessage())
}

func (c *client) sendRawMsg(msg *primitive.Message) (err error) {
	var ret *primitive.SendResult
	for i := 0; i < sendRetry; i++ {
		if i > 0 {
			time.Sleep(retryInterval * time.Duration(i))
		}
		ret, err = c.pub.rkp.SendSync(context.Background(), msg)
		if err == nil {
			goto NEXT
		}
	}
	if err != nil {
		return
	}
NEXT:
	if ret.Status != primitive.SendOK {
		err = ErrSendFailed
		log.Error(err.Error(), zap.String("ret", ret.String()))
	}
	return
}

// 响应RPC请求（仅NewSrvClient生成的客户端可用）
func (c *client) Respond(msg *Message) (err error) {
	if err = c.checkRespMsg(msg); err != nil {
		return
	}
	reply, err := consumer.CreateReplyMessage(msg.rawMsg, nil)
	if err != nil {
		return
	}
	reply.WithTag(msg.Tag).WithKeys(msg.Keys)
	reply.Body = msg.Body
	err = c.sendRawMsg(reply)
	return
}

func (c *client) getReqCtx(msg *Message, ctx ...context.Context) (tx context.Context, ttl time.Duration, req *primitive.Message, err error) {
	if err = c.checkReqMsg(msg); err != nil {
		return
	}
	tx = context.Background()
	ttl = c.rpcTTL
	if len(ctx) > 0 {
		tx = ctx[0]
		if diff := GetTTLFromContext(tx); diff > 0 {
			ttl = diff
		}
	}
	req = primitive.NewMessage(msg.Topic, msg.Body).WithTag(msg.Tag).WithKeys(msg.Keys)
	return
}

// 同步阻塞RPC（ctx中可以设置timeout，其优先级高于client中的ttl）
func (c *client) Request(msg *Message, ctx ...context.Context) (resp *Message, err error) {
	tx, ttl, req, err := c.getReqCtx(msg, ctx...)
	if err != nil {
		return
	}
	m, err := c.pub.rkp.Request(tx, ttl, req)
	if err != nil {
		return
	}
	resp = msgFromRkMsg(m)
	resp.RemoteApp = msg.RemoteApp
	log.Info("sync request succeed", zap.String("tag", resp.Tag), zap.Any("keys", resp.Keys))
	return
}

// 异步接收RPC响应（通过输入的函数处理响应体）
func (c *client) RequestAsyncWithFunc(msg *Message, callback RpcCallback, ctx ...context.Context) (err error) {
	tx, ttl, req, err := c.getReqCtx(msg, ctx...)
	if err != nil {
		return
	}
	err = c.pub.rkp.RequestAsync(tx, ttl, func(ctx context.Context, m *primitive.Message, e error) {
		if e != nil {
			log.Error("async request failed", zap.Error(e))
			return
		}
		resp := msgFromRkMsg(m)
		resp.RemoteApp = msg.RemoteApp
		callback(resp)
		log.Info("async request succeed", zap.String("tag", resp.Tag), zap.Any("keys", resp.Keys))
	}, req)
	return
}

// 异步接收RPC响应（通过输入的chan接收响应体）
func (c *client) RequestAsync(msg *Message, out chan *Message, ctx ...context.Context) (err error) {
	err = c.RequestAsyncWithFunc(msg, func(m *Message) {
		out <- m
	}, ctx...)
	return
}
