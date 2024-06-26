package mqlib

import (
	"context"
	"os"

	"github.com/wgdzlh/mqlib/log"

	"github.com/wgdzlh/mqlib/rk/admin"
	"github.com/wgdzlh/mqlib/rk/primitive"
	"go.uber.org/zap"
)

const (
	BROKER_ADDR_ENV = "MQLIB_BROKER_ADDR"
)

func CreateTopic(nsName string, topics ...string) (err error) {
	brokerAddr := "broker-a.rocketmq.svc.cluster.local:10911"
	if realBrokerAddr := os.Getenv(BROKER_ADDR_ENV); realBrokerAddr != "" {
		brokerAddr = realBrokerAddr
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
			log.Warn("auto create topic failed, ignore this or change broker addr with env MQLIB_BROKER_ADDR",
				zap.String("topic", topic), zap.Error(err))
			return
		}
	}
	return
}
