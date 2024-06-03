package mqlib

import (
	"context"
	"os"

	"github.com/wgdzlh/mqlib/log"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
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
	log.Info("creating new topic", zap.String("brokerAddr", brokerAddr), zap.Any("topics", topics))
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
			admin.WithBrokerAddrCreate(brokerAddr),
		); err != nil {
			log.Error("create new topic failed", zap.Error(err))
			return
		}
	}
	return
}
