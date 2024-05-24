module github.com/wgdzlh/mqlib

go 1.16

require (
	github.com/apache/rocketmq-client-go/v2 v2.1.2
	github.com/google/uuid v1.3.0
	github.com/json-iterator/go v1.1.12
	github.com/patrickmn/go-cache v2.1.0+incompatible
	go.uber.org/zap v1.23.0
)

replace github.com/apache/rocketmq-client-go/v2 v2.1.2 => github.com/wgdzlh/rocketmq-client-go/v2 v2.1.2-f1
