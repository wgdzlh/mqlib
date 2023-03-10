package mqlib

import (
	"context"
	"errors"
	"fmt"
	"mqlib/log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"go.uber.org/zap"
)

const (
	subGpSuffix     = "-sub-gp"
	pubGpSuffix     = "-pub-gp"
	reqTopicSuffix  = "-req"
	respTopicSuffix = "-resp"
)

var (
	ErrSendFailed   = errors.New("failed to send msg")
	ErrFetchFailed  = errors.New("failed to fetch msg")
	ErrMisformedMsg = errors.New("input msg is invalid")
)

type Client struct {
	Name       string    // MQ客户端名称（即本服务名称）
	nameServer string    // MQ的NameServer地址
	subGpName  string    // MQ消费者分组名称
	sub        *Consumer // 可接收RPC调用的MQ客户端消费者
	pub        *Producer // MQ客户端通用的生产者
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
func NewSrvClient(nameServer, app string, subDispatcher SubDispatcher) (c *Client, err error) {
	if c, err = NewClient(nameServer, app); err != nil {
		return
	}
	c.sub, err = NewConsumer(c.subGpName, nameServer, c.getSrvSubTopic(subDispatcher))
	return
}

// 只做RPC调用的MQ客户端
func NewClient(nameServer, app string) (c *Client, err error) {
	c = &Client{
		Name:       app,
		nameServer: nameServer,
		subGpName:  app + subGpSuffix,
	}
	c.pub, err = NewProducer(app+pubGpSuffix, nameServer)
	return
}

func (c *Client) getSrvSubTopic(dispatcher SubDispatcher) Topic {
	return Topic{
		Name:   c.Name + reqTopicSuffix,
		Filter: consumer.MessageSelector{},
		Callback: func(ctx context.Context, me ...*primitive.MessageExt) (ret consumer.ConsumeResult, err error) {
			var keys []string
			for _, m := range me {
				keys = strings.Split(strings.TrimSuffix(m.GetKeys(), primitive.PropertyKeySeparator), primitive.PropertyKeySeparator)
				if err = dispatcher.ProcessMsg(
					&Message{
						Tag:  m.GetTags(),
						Keys: keys,
						Body: m.Body}); err != nil {
					ret = consumer.ConsumeRetryLater
					return
				}
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

// 响应RPC请求
func (c *Client) Respond(msg *Message) (err error) {
	if err = c.checkMsg(msg); err != nil {
		return
	}
	msg.topic = c.Name + respTopicSuffix
	err = c.sendMsg(msg)
	return
}

// 发送一次性消息（RPC请求）
func (c *Client) Send(msg *Message) (err error) {
	reqId := getUniqKey()
	msg.topic = msg.RemoteApp + reqTopicSuffix
	msg.Keys = append(msg.Keys, reqId)
	err = c.sendMsg(msg)
	return
}

// 同步阻塞接收一次性消息（获取RPC响应）
func (c *Client) Fetch(msg *Message) (resp []byte, err error) {
	if err = c.checkMsg(msg); err != nil {
		return
	}
	var (
		key  = msg.Keys[len(msg.Keys)-1]
		done = make(chan struct{})
	)
	consumer, err := NewConsumer(c.subGpName, c.nameServer, Topic{
		Name: msg.RemoteApp + respTopicSuffix,
		Filter: consumer.MessageSelector{
			Type:       consumer.SQL92,
			Expression: fmt.Sprintf(`TAGS = '%s' AND KEYS = '%s'`, msg.Tag, key),
		},
		Callback: func(ctx context.Context, me ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			defer close(done)
			for _, m := range me {
				resp = m.Body
				break
			}
			return consumer.ConsumeSuccess, nil
		},
	})
	if err != nil {
		return
	}
	defer consumer.rkc.Shutdown()
	<-done
	if len(resp) == 0 {
		err = ErrFetchFailed
	}
	return
}
