package mqlib

import (
	"context"

	"github.com/wgdzlh/mqlib/log"

	rocketmq "github.com/wgdzlh/mqlib/rk"
	"github.com/wgdzlh/mqlib/rk/consumer"
	"github.com/wgdzlh/mqlib/rk/primitive"
	"github.com/wgdzlh/mqlib/rk/producer"
	"go.uber.org/zap"
)

const (
	DEFAULT_RETRY = 3
)

type realCallback = func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)

type Topic struct {
	Name     string
	Filter   consumer.MessageSelector
	Callback SubCallback
}

type Producer struct {
	GroupName string
	rkp       rocketmq.Producer
}

func NewProducer(gpName, nsName string) (p *Producer, err error) {
	p = &Producer{
		GroupName: gpName,
	}
	if p.rkp, err = rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{nsName})),
		producer.WithGroupName(gpName),
		producer.WithRetry(DEFAULT_RETRY),
	); err != nil {
		log.Error("init rocketmq producer failed", zap.Error(err))
		return
	}
	if err = p.rkp.Start(); err != nil {
		log.Error("start rocketmq producer failed", zap.Error(err))
	}
	return
}

func (p *Producer) Shutdown() {
	p.rkp.Shutdown()
}

type Consumer struct {
	GroupName string
	Topics    []Topic
	rkc       rocketmq.PushConsumer
}

func NewConsumer(gpName, nsName string, unitMode, broadcast bool, topics ...Topic) (c *Consumer, err error) {
	c = &Consumer{
		GroupName: gpName,
		Topics:    topics,
	}
	unitName := ""
	if unitMode {
		unitName = getUnitName()
	}
	consumerModel := consumer.Clustering
	if broadcast {
		consumerModel = consumer.BroadCasting
	}
	if c.rkc, err = rocketmq.NewPushConsumer(
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{nsName})),
		consumer.WithUnitName(unitName),
		consumer.WithGroupName(gpName),
		consumer.WithConsumerModel(consumerModel),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		consumer.WithRetry(DEFAULT_RETRY),
	); err != nil {
		log.Error("init rocketmq consumer failed", zap.Error(err))
		return
	}
	for _, t := range c.Topics {
		if err = c.rkc.Subscribe(t.Name, t.Filter, getRealCallback(t.Callback)); err != nil {
			log.Error("subscribe failed", zap.String("topic", t.Name), zap.String("filter", t.Filter.Expression), zap.Error(err))
			return
		}
	}
	if err = c.rkc.Start(); err != nil {
		log.Error("start rocketmq consumer failed", zap.Error(err))
	}
	return
}

func getRealCallback(sc SubCallback) realCallback {
	return func(ctx context.Context, me ...*primitive.MessageExt) (ret consumer.ConsumeResult, err error) {
		for _, m := range me {
			if err = sc(msgFromRkMsgExt(m)); err != nil {
				ret = consumer.ConsumeRetryLater
				return
			}
		}
		return consumer.ConsumeSuccess, nil
	}
}

func (c *Consumer) Shutdown() {
	c.rkc.Shutdown()
}
