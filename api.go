package mqlib

type Message struct {
	topic     string   // 消息主题（自动配置）
	RemoteApp string   // RPC调用服务端应用名称
	Tag       string   // RPC调用接口名称
	Keys      []string // 携带的业务KEY（可选）
	Body      []byte   // 业务消息体
}

type SubDispatcher interface {
	ProcessMsg(msg *Message) error
}
