package mqlib

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/wgdzlh/mqlib/log"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"go.uber.org/zap"
)

const (
	pubGpSuffix    = "-pub-gp"
	subGpSuffix    = "-sub-gp"
	rpcTopicSuffix = "-rpc"
	defaultRpcTTL  = time.Minute * 10 // 默认rpc请求timeout
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

type Client struct {
	Name       string        // MQ客户端名称（即本服务名称）
	nameServer string        // MQ的NameServer地址
	pub        *Producer     // MQ客户端公用的生产者
	sub        *Consumer     // 可接收RPC调用的MQ客户端消费者
	rpcTTL     time.Duration // RPC请求的timeout
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

// 可接收RPC调用的MQ客户端
func NewSrvClient(nameServer, app string, subDispatcher SubDispatcher, ttl ...time.Duration) (c *Client, err error) {
	if c, err = NewClient(nameServer, app, ttl...); err != nil {
		return
	}
	apiSubTopic := c.getSrvSubTopic(subDispatcher)
	c.sub, err = NewConsumer(app+subGpSuffix, nameServer, apiSubTopic)
	return
}

// 只做RPC调用的MQ客户端
func NewClient(nameServer, app string, ttl ...time.Duration) (c *Client, err error) {
	c = &Client{
		Name:       app,
		nameServer: nameServer,
	}
	if len(ttl) > 0 && ttl[0] > 0 {
		c.rpcTTL = ttl[0]
	} else {
		c.rpcTTL = defaultRpcTTL
	}
	c.pub, err = NewProducer(app+pubGpSuffix, nameServer)
	return
}

func (c *Client) SetRpcTimeout(ttl time.Duration) {
	if ttl > 0 {
		c.rpcTTL = ttl
	}
}

func (c *Client) getSrvSubTopic(dispatcher SubDispatcher) Topic {
	return Topic{
		Name:   c.Name + rpcTopicSuffix,
		Filter: consumer.MessageSelector{},
		Callback: func(me *primitive.MessageExt) (consumer.ConsumeResult, error) {
			err := dispatcher.ProcessMsg(msgFromRkMsgExt(me))
			if err != nil {
				return consumer.ConsumeRetryLater, err
			}
			return consumer.ConsumeSuccess, nil
		},
	}
}

func (c *Client) checkReqMsg(msg *Message) (err error) {
	if msg.RemoteApp == "" || msg.Tag == "" || msg.Topic != "" {
		err = ErrMisformedMsg
	}
	msg.Topic = msg.RemoteApp + rpcTopicSuffix
	return
}

func (c *Client) checkRespMsg(msg *Message) (err error) {
	if msg.rawMsg == nil {
		err = ErrMisformedMsg
	}
	return
}

func (c *Client) SendMessage(msg *Message) (err error) {
	return c.sendRawMsg(msg.ToRkMessage())
}

func (c *Client) sendRawMsg(msg *primitive.Message) (err error) {
	ret, err := c.pub.rkp.SendSync(context.Background(), msg)
	if err != nil {
		return
	}
	if ret.Status != primitive.SendOK {
		err = ErrSendFailed
		log.Error(err.Error(), zap.String("ret", ret.String()))
	}
	return
}

// 响应RPC请求（仅NewSrvClient生成的客户端可用）
func (c *Client) Respond(msg *Message) (err error) {
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

func (c *Client) getReqCtx(msg *Message, ctx ...context.Context) (tx context.Context, ttl time.Duration, req *primitive.Message, err error) {
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

// 同步阻塞RPC
func (c *Client) Request(msg *Message, ctx ...context.Context) (resp *Message, err error) {
	tx, ttl, req, err := c.getReqCtx(msg, ctx...)
	if err != nil {
		return
	}
	respM, err := c.pub.rkp.Request(tx, ttl, req)
	if err != nil {
		return
	}
	resp = msgFromRkMsg(respM)
	resp.RemoteApp = msg.RemoteApp
	return
}

// 异步接收RPC响应（通过输入的函数处理响应体）
func (c *Client) RequestAsyncWithFunc(msg *Message, callback RpcCallback, ctx ...context.Context) (err error) {
	tx, ttl, req, err := c.getReqCtx(msg, ctx...)
	if err != nil {
		return
	}
	var resp *Message
	err = c.pub.rkp.RequestAsync(tx, ttl, func(ctx context.Context, m *primitive.Message, e error) {
		if resp != nil { // 保证callback只调用一次（临时方案，SDK有bug待修正）
			return
		}
		if e != nil {
			log.Error("async request failed", zap.Error(e))
			return
		}
		resp = msgFromRkMsg(m)
		resp.RemoteApp = msg.RemoteApp
		callback(resp)
		log.Info("async request succeed", zap.Any("keys", resp.Keys), zap.String("reqId", m.GetProperty(primitive.PropertyCorrelationID)))
	}, req)
	return
}

// 异步接收RPC响应（通过输入的chan接收响应体）
func (c *Client) RequestAsync(msg *Message, out chan *Message, ctx ...context.Context) (err error) {
	err = c.RequestAsyncWithFunc(msg, func(m *Message) {
		out <- m
	}, ctx...)
	return
}
