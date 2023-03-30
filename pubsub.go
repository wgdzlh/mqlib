package mqlib

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/wgdzlh/mqlib/log"
	rocketmq "github.com/wgdzlh/mqlib/rk"
	"github.com/wgdzlh/mqlib/rk/consumer"
	"github.com/wgdzlh/mqlib/rk/primitive"
	"github.com/wgdzlh/mqlib/rk/producer"

	"go.uber.org/zap"
)

const (
	DEFAULT_RETRY = 3
	tagSep        = "||"
)

var (
	ErrNewTopicAfterStart = errors.New("add new topic after consumer started")
)

type realCallback = func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)

type Topic struct {
	Name     string      // 订阅的topic名称
	Tags     []string    // 订阅的topic下的tag列表，可留空（表示订阅所有tag），订阅后仍可以修改（通过Consumer.AddTagsToSubbedTopic函数），但响应消息有被过滤风险
	Callback SubCallback // 订阅的topic+tag收到消息时的回调，在第一次订阅topic时指定，订阅后不可修改
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

type subData struct {
	sync.RWMutex
	topic Topic
	tags  map[string]struct{}
}

type Consumer struct {
	sync.RWMutex
	GroupName string
	subMap    map[string]*subData
	rkc       rocketmq.PushConsumer
	started   bool
}

func NewConsumer(gpName, nsName string, broadcast bool, topics ...Topic) (c *Consumer, err error) {
	c = &Consumer{
		GroupName: gpName,
		subMap:    map[string]*subData{},
	}
	consumerModel := consumer.Clustering
	unitName := ""
	if broadcast {
		consumerModel = consumer.BroadCasting
		unitName = getUnitName()
	}
	if c.rkc, err = rocketmq.NewPushConsumer(
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{nsName})),
		consumer.WithUnitName(unitName),
		consumer.WithGroupName(gpName),
		consumer.WithConsumerModel(consumerModel),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		consumer.WithPostSubscriptionWhenPull(!broadcast),
		consumer.WithRetry(DEFAULT_RETRY),
	); err != nil {
		log.Error("init rocketmq consumer failed", zap.Error(err))
		return
	}
	if err = c.subscribe(topics...); err != nil {
		log.Error("subscribe failed", zap.Error(err))
		return
	}
	if err = c.rkc.Start(); err != nil {
		log.Error("start rocketmq consumer failed", zap.Error(err))
	}
	c.started = true
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

func (c *Consumer) subscribe(topics ...Topic) (err error) {
	c.Lock()
	defer c.Unlock()
	for _, topic := range topics {
		changed := false
		sub, ok := c.subMap[topic.Name]
		if ok {
			sub.Lock()
			for _, tag := range topic.Tags {
				if _, ok = sub.tags[tag]; !ok {
					sub.tags[tag] = struct{}{}
					changed = true
				}
			}
			sub.Unlock()
		} else {
			if c.started {
				err = ErrNewTopicAfterStart
				return
			}
			sub = &subData{
				topic: topic,
				tags:  map[string]struct{}{},
			}
			for _, tag := range topic.Tags {
				sub.tags[tag] = struct{}{}
			}
			c.subMap[topic.Name] = sub
			changed = true
		}
		if changed {
			tagList := sub.getJoinedTags()
			log.Info("new topic tags to subscribe", zap.String("gp", c.GroupName),
				zap.String("topic", topic.Name), zap.String("tags", tagList))
			if err = c.rkc.Subscribe(topic.Name, consumer.MessageSelector{
				Type:       consumer.TAG,
				Expression: tagList,
			}, getRealCallback(sub.topic.Callback)); err != nil {
				return
			}
		}
	}
	return
}

// 给已订阅的topic修改tag，这里注意如果对应的响应消息很快就发出来的话（在默认的长轮询20s时间间隔以内），
// 该消息可能会被过滤掉，导致接收不到
func (c *Consumer) AddTagsToSubbedTopic(topic string, tags ...string) error {
	if len(tags) == 0 {
		return nil
	}
	return c.subscribe(Topic{
		Name: topic,
		Tags: tags,
	})
}

func (c *Consumer) UnsubscribeTopic(topic string) error {
	c.Lock()
	delete(c.subMap, topic)
	c.Unlock()
	return c.rkc.Unsubscribe(topic)
}

func (c *Consumer) UnsubscribeTags(topic string, tags ...string) (err error) {
	c.RLock()
	if sub, ok := c.subMap[topic]; ok {
		changed := false
		sub.Lock()
		for _, tag := range tags {
			if _, ok = sub.tags[tag]; ok {
				delete(sub.tags, tag)
				changed = true
			}
		}
		sub.Unlock()
		if changed {
			tagList := sub.getJoinedTags()
			log.Info("unsubscribe topic tags", zap.String("gp", c.GroupName),
				zap.String("topic", topic), zap.String("tags", tagList), zap.Any("removed", tags))
			err = c.rkc.Subscribe(topic, consumer.MessageSelector{
				Type:       consumer.TAG,
				Expression: tagList,
			}, getRealCallback(sub.topic.Callback))
		}
	}
	c.RUnlock()
	return
}

func (c *Consumer) SubscribeAllTags(topic string) error {
	c.RLock()
	sub, ok := c.subMap[topic]
	c.RUnlock()
	if !ok {
		return nil
	}
	return c.UnsubscribeTags(topic, sub.getTags()...)
}

func (s *subData) getTags() []string {
	s.RLock()
	keys := make([]string, len(s.tags))
	i := 0
	for k := range s.tags {
		keys[i] = k
		i++
	}
	s.RUnlock()
	return keys
}

func (s *subData) getJoinedTags() string {
	if len(s.tags) == 0 {
		return ""
	}
	return strings.Join(s.getTags(), tagSep)
}
