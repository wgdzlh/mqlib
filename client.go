package mqlib

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wgdzlh/mqlib/log"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"go.uber.org/zap"
)

const (
	subGpSuffix      = "-sub-gp"
	pubGpSuffix      = "-pub-gp"
	reqTopicSuffix   = "-req"
	respTopicSuffix  = "-resp"
	tagKeySep        = "@"
	asyncConsumerMin = 128  // 默认并发异步消费者上限
	asyncConsumerMax = 2048 // 默认并发异步消费者上限
)

var (
	ErrSendFailed   = errors.New("failed to send msg")
	ErrFetchFailed  = errors.New("failed to fetch msg")
	ErrMisformedMsg = errors.New("input msg is invalid")
	ErrMsgTimeout   = errors.New("msg timeout")
	ErrReachedLimit = errors.New("reached concurrent limit")
)

type Client struct {
	Name          string         // MQ客户端名称（即本服务名称）
	nameServer    string         // MQ的NameServer地址
	subGpName     string         // MQ消费者分组名称
	sub           *Consumer      // 可接收RPC调用的MQ客户端消费者
	pub           *Producer      // MQ客户端通用的生产者
	usedConsumers chan *Consumer // 使用过的消费者队列
	asyncLimit    int            // 使用过的消费者队列容量
	asyncOnce     sync.Once
}

func init() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	rlog.SetLogLevel("warn")
	rlog.SetOutputPath(filepath.Join(exPath, "rocketmq.log"))
}

// 可接收RPC调用的MQ客户端
func NewSrvClient(nameServer, app string, subDispatcher SubDispatcher, asyncLimit ...int) (c *Client, err error) {
	if c, err = NewClient(nameServer, app, asyncLimit...); err != nil {
		return
	}
	apiSubTopic := c.getSrvSubTopic(subDispatcher)
	c.sub, err = NewConsumer(c.subGpName, nameServer, nil, apiSubTopic)
	return
}

// 只做RPC调用的MQ客户端
func NewClient(nameServer, app string, asyncLimit ...int) (c *Client, err error) {
	c = &Client{
		Name:       app,
		nameServer: nameServer,
		subGpName:  app + subGpSuffix,
	}
	if len(asyncLimit) > 0 {
		if asyncLimit[0] < 0 {
			c.asyncLimit = asyncConsumerMax
		} else if asyncLimit[0] == 0 {
			c.asyncLimit = asyncConsumerMin
		} else {
			c.asyncLimit = asyncLimit[0]
		}
	}
	c.pub, err = NewProducer(app+pubGpSuffix, nameServer)
	return
}

func (c *Client) getSrvSubTopic(dispatcher SubDispatcher) Topic {
	return Topic{
		Name:   c.Name + reqTopicSuffix,
		Filter: consumer.MessageSelector{},
		Callback: func(m *primitive.MessageExt) (ret consumer.ConsumeResult, err error) {
			keys := strings.Split(strings.TrimSuffix(m.GetKeys(), primitive.PropertyKeySeparator), primitive.PropertyKeySeparator)
			if err = dispatcher.ProcessMsg(
				&Message{
					Tag:  m.GetTags(),
					Keys: keys,
					Body: m.Body}); err != nil {
				ret = consumer.ConsumeRetryLater
				return
			}
			return consumer.ConsumeSuccess, nil
		},
	}
}

func (c *Client) checkMsg(msg *Message) (err error) {
	if msg.Tag == "" || len(msg.Keys) == 0 {
		err = ErrMisformedMsg
	}
	return
}

func (c *Client) sendMsg(msg *Message) (err error) {
	ret, err := c.pub.rkp.SendSync(context.Background(), primitive.NewMessage(msg.topic, msg.Body).WithTag(msg.Tag).WithKeys(msg.Keys))
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
	if err = c.checkMsg(msg); err != nil {
		return
	}
	msg.topic = c.Name + respTopicSuffix
	msg.Tag += tagKeySep + msg.Keys[0] // 将reqId与tag进行拼接，避免rocketMQ不支持sql92过滤时，在消费端无法过滤消息
	msg.Keys = msg.Keys[1:]
	err = c.sendMsg(msg)
	return
}

// 发送RPC请求消息
func (c *Client) Send(msg *Message) (err error) {
	reqId := getUniqKey()
	msg.Keys = append([]string{reqId}, msg.Keys...) // 将reqId（uuid）插入消息KEYS属性中，方便获取响应
	msg.topic = msg.RemoteApp + reqTopicSuffix
	err = c.sendMsg(msg)
	return
}

// 同步阻塞接收RPC响应
func (c *Client) Fetch(msg *Message, timeout ...time.Duration) (resp []byte, err error) {
	if err = c.checkMsg(msg); err != nil {
		return
	}
	var (
		key  = msg.Keys[0]
		done = make(sig) // done信号在这里生成，可以灵活控制异步的消费流程
	)
	csm, err := NewConsumer(c.subGpName, c.nameServer, done, Topic{
		Name: msg.RemoteApp + respTopicSuffix,
		Filter: consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: msg.Tag + tagKeySep + key, // 用来过滤消息的tag组合
		},
		Callback: func(m *primitive.MessageExt) (consumer.ConsumeResult, error) {
			resp = m.Body
			return consumer.ConsumeSuccess, nil
		},
	})
	if err != nil {
		return
	}
	defer csm.rkc.Shutdown() // 同步调用时，自己控制消费者的清理
	if len(timeout) > 0 && timeout[0] > 0 {
		select {
		case <-done:
		case <-time.After(timeout[0]):
			err = ErrMsgTimeout
		}
	} else {
		<-done
	}
	if err == nil && len(resp) == 0 {
		err = ErrFetchFailed
	}
	return
}

func (c *Client) consumeAsync(msg *Message, callback SubCallback) (err error) {
	if err = c.checkMsg(msg); err != nil {
		return
	}
	var (
		key  = msg.Keys[0]
		done = make(chan struct{})
	)
	csm, err := NewConsumer(c.subGpName, c.nameServer, done, Topic{
		Name: msg.RemoteApp + respTopicSuffix,
		Filter: consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: msg.Tag + tagKeySep + key, // 用来过滤消息的tag组合
		},
		Callback: callback,
	})
	if err != nil {
		return
	}
	c.triggerAsyncGc()
	select {
	case c.usedConsumers <- csm: // 异步调用时，由后台协程清理消费者
	default:
		err = ErrReachedLimit
	}
	return
}

// 异步接收RPC响应（通过输入的chan接收响应体）
func (c *Client) FetchAsync(msg *Message, out chan []byte) (err error) {
	err = c.consumeAsync(msg, func(m *primitive.MessageExt) (consumer.ConsumeResult, error) {
		out <- m.Body
		return consumer.ConsumeSuccess, nil
	})
	return
}

// 异步接收RPC响应（通过输入的函数处理响应体）
func (c *Client) FetchAsyncWithFunc(msg *Message, f func(body []byte) error) (err error) {
	err = c.consumeAsync(msg, func(m *primitive.MessageExt) (consumer.ConsumeResult, error) {
		if err := f(m.Body); err != nil {
			return consumer.ConsumeRetryLater, nil
		}
		return consumer.ConsumeSuccess, nil
	})
	return
}

func (c *Client) triggerAsyncGc() {
	c.asyncOnce.Do(func() {
		c.usedConsumers = make(chan *Consumer, c.asyncLimit)
		go func() {
			for csm := range c.usedConsumers {
				<-csm.done
				time.Sleep(time.Millisecond * 1) // 这里延缓1ms，避免过早关闭消费者造成消费结果未回报
				csm.rkc.Shutdown()
			}
		}()
	})
}
