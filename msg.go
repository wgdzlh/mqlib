package mqlib

import (
	"strings"

	json "github.com/json-iterator/go"
	"github.com/wgdzlh/mqlib/rk/primitive"
)

var (
	emptyBody = []byte("-")
)

type Message struct {
	Id        string                `json:"id,omitempty"`         // 消息ID
	RemoteApp string                `json:"remote_app,omitempty"` // RPC调用服务端应用名称
	Topic     string                `json:"topic,omitempty"`      // 消息主题（可选）
	Tag       string                `json:"tag"`                  // RPC调用接口名称
	Keys      []string              `json:"keys"`                 // 携带的业务KEY（可选）
	Body      []byte                `json:"-"`                    // 业务消息体
	rawMsg    *primitive.MessageExt `json:"-"`                    // 原始MQ消息，自动处理，勿动
}

func (m *Message) ToString() string {
	out, _ := json.MarshalToString(m)
	return out
}

func (m *Message) ToRkMessage() *primitive.Message {
	if len(m.Body) == 0 {
		m.Body = emptyBody
	}
	return primitive.NewMessage(m.Topic, m.Body).WithTag(m.Tag).WithKeys(m.Keys)
}

func msgFromRkMsg(m *primitive.Message) *Message {
	return &Message{
		Tag:  m.GetTags(),
		Keys: getKeysFromMsg(m),
		Body: m.Body,
	}
}

func msgFromRkMsgExt(me *primitive.MessageExt) *Message {
	return &Message{
		Id:     me.MsgId,
		Topic:  me.Topic,
		Tag:    me.GetTags(),
		Keys:   getKeysFromMsg(&me.Message),
		Body:   me.Body,
		rawMsg: me,
	}
}

func getKeysFromMsg(m *primitive.Message) []string {
	var keys []string
	rawKey := m.GetKeys()
	if rawKey != "" {
		keys = strings.Split(rawKey, primitive.PropertyKeySeparator)
		last := len(keys) - 1
		if last > 0 && keys[last] == "" {
			keys = keys[:last]
		}
	}
	return keys
}
