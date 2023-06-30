package mqlib

import (
	"context"

	"github.com/wgdzlh/mqlib/log"
	"github.com/wgdzlh/mqlib/rk/primitive"
	"go.uber.org/zap"
)

func CreateTopic(nsName string, topics ...string) (err error) {
	log.Info("creating new topic", zap.Any("topics", topics))
	tmpPrd, err := NewProducer("tmp-admin", nsName)
	if err != nil {
		log.Error("create admin producer failed", zap.Error(err))
		return
	}
	defer tmpPrd.Shutdown()
	for _, topic := range topics {
		if _, err = tmpPrd.rkp.SendSync(context.Background(), &primitive.Message{
			Topic: topic,
		}); err != nil {
			log.Error("send msg to new topic failed", zap.Error(err))
			return
		}
	}
	return
}
