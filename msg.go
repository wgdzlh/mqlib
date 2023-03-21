package mqlib

import (
	"strings"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	jsoniter "github.com/json-iterator/go"
)

type Message struct {
	RemoteApp string                `json:"remote_app,omitempty"` // RPC调用服务端应用名称
	Topic     string                `json:"topic,omitempty"`      // 消息主题（可选）
	Tag       string                `json:"tag"`                  // RPC调用接口名称
	Keys      []string              `json:"keys"`                 // 携带的业务KEY（可选）
	Body      []byte                `json:"-"`                    // 业务消息体
	rawMsg    *primitive.MessageExt `json:"-"`                    // 原始MQ消息，自动处理，勿动
}

func (m *Message) ToString() string {
	out, _ := jsoniter.MarshalToString(m)
	return out
}

func (m *Message) ToRkMessage() *primitive.Message {
	return primitive.NewMessage(m.Topic, m.Body).WithTag(m.Tag).WithKeys(m.Keys)
}

func msgFromRkMsg(m *primitive.Message) *Message {
	keys := strings.Split(strings.TrimSuffix(m.GetKeys(), primitive.PropertyKeySeparator), primitive.PropertyKeySeparator)
	return &Message{
		Tag:  m.GetTags(),
		Keys: keys,
		Body: m.Body,
	}
}

func msgFromRkMsgExt(me *primitive.MessageExt) *Message {
	keys := strings.Split(strings.TrimSuffix(me.GetKeys(), primitive.PropertyKeySeparator), primitive.PropertyKeySeparator)
	return &Message{
		Topic:  me.Topic,
		Tag:    me.GetTags(),
		Keys:   keys,
		Body:   me.Body,
		rawMsg: me,
	}
}
