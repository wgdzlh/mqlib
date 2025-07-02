package mqlib

import (
	"context"

	"github.com/wgdzlh/mqlib/log"

	"github.com/wgdzlh/mqlib/rk/admin"
	"github.com/wgdzlh/mqlib/rk/primitive"
	"go.uber.org/zap"
)

var (
	brokerAddr = "broker-a.rocketmq.svc.cluster.local:10911"
)

func CreateTopics(nsName string, topics []string, brokerAddrCfg ...string) (err error) {
	if len(brokerAddrCfg) > 0 && brokerAddrCfg[0] != "" {
		brokerAddr = brokerAddrCfg[0]
	}
	log.Info("creating topics if not exist", zap.String("brokerAddr", brokerAddr), zap.Any("topics", topics))
	if len(topics) == 0 {
		return
	}
	mqAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver([]string{nsName})),
	)
	if err != nil {
		log.Error("create admin instance failed", zap.Error(err))
		return
	}
	defer mqAdmin.Close()
	for _, topic := range topics {
		if err = mqAdmin.CreateTopic(
			context.Background(),
			admin.WithTopicCreate(topic),
			admin.WithReadQueueNums(4),
			admin.WithWriteQueueNums(4),
			admin.WithBrokerAddrCreate(brokerAddr),
		); err != nil {
			log.Warn("auto create topic failed, ignore this or set brokerAddrCfg",
				zap.String("topic", topic), zap.Error(err))
			return
		}
	}
	return
}
