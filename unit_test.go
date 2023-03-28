package mqlib

import (
	"bytes"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	nameServer = "http://localhost:9876"
	app1       = "mqlib-test-1"
	app2       = "mqlib-test-2"
	tag1       = "api-1"
	queSize    = 128
	queTimeout = time.Second * 5
	procTime   = time.Second * 1
)

var respSuffix = []byte("---response")

type SomeService struct {
	msgQue     chan *Message
	mqClient   RpcSrvClient
	msgProTime time.Duration
}

func newService() *SomeService {
	srv := &SomeService{
		msgQue:     make(chan *Message, queSize),
		msgProTime: procTime,
	}
	srv.startMsgLoop()
	return srv
}

func (s *SomeService) ProcessMsg(msg *Message) (err error) {
	select {
	case s.msgQue <- msg:
	case <-time.After(queTimeout):
		err = errors.New("queue timeout")
	}
	return
}

func (s *SomeService) startMsgLoop() {
	// 这里可以起多个loop，提高并发，降低总体延迟
	for i := 0; i < 2; i++ {
		go func() {
			for msg := range s.msgQue {
				log.Printf("ss got message: %s", msg.ToString())
				switch msg.Tag {
				case tag1:
					time.Sleep(s.msgProTime)
					msg.Body = append(msg.Body, respSuffix...)
					s.mqClient.Respond(msg)
				default:
					log.Print("unknown api tag:", msg.Tag)
					continue
				}
				log.Printf("ss finish message: %s", msg.ToString())
			}
		}()
	}
}

func getRandKey() string {
	return strconv.FormatInt(rand.Int63n(10000)+10000, 10)
}

func TestAllRPC(t *testing.T) {
	// 首先在rocketmq上新建topic：mqlib-test-1-rpc
	var (
		s   = newService() // RPC服务端
		c   PubClient      // RPC客户端
		err error
	)
	if s.mqClient, err = NewSrvClient(nameServer, app1, s); err != nil {
		t.Fatal(err)
	}
	if c, err = NewClient(nameServer, app2); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	defer s.mqClient.Shutdown()
	// sync
	msg := &Message{
		RemoteApp: app1,
		Tag:       tag1,
		Keys:      []string{getRandKey()},
		Body:      []byte("hello world 1"),
	}
	m, err := c.Request(msg)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(m.Body, append(msg.Body, respSuffix...)) {
		t.Logf("rpc 1 succeed with response: %s, keys: %s", m.Body, m.Keys)
	} else {
		t.Errorf("unexpected rpc response: %s", m.Body)
	}
	// async
	msg = &Message{
		RemoteApp: app1,
		Tag:       tag1,
		Keys:      []string{getRandKey()},
		Body:      []byte("hello world 2"),
	}
	resChan := make(chan *Message, 1)
	if err = c.RequestAsync(msg, resChan); err != nil {
		t.Fatal(err)
	}
	m = <-resChan
	if bytes.Equal(m.Body, append(msg.Body, respSuffix...)) {
		t.Logf("rpc 2 succeed with response: %s, keys: %s", m.Body, m.Keys)
	} else {
		t.Errorf("unexpected rpc response: %s", m.Body)
	}
	// async func
	msg = &Message{
		RemoteApp: app1,
		Tag:       tag1,
		Keys:      []string{getRandKey()},
		Body:      []byte("hello world 3"),
	}
	if err = c.RequestAsyncWithFunc(msg, func(m *Message) {
		if bytes.Equal(m.Body, append(msg.Body, respSuffix...)) {
			t.Logf("rpc 3 succeed with response: %s, keys: %s", m.Body, m.Keys)
		} else {
			t.Errorf("unexpected rpc response: %s", m.Body)
		}
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 6) // sleep达到较长时间，才能将SomeService消费的offset同步到MQ
	t.Log("all test finished")
}

func TestConcurrentSyncRPC(t *testing.T) {
	// 首先在rocketmq上新建topic：mqlib-test-1-rpc
	var (
		s   = newService() // RPC服务端
		c   PubClient      // RPC客户端
		wg  sync.WaitGroup
		n   = 10
		err error
	)
	if s.mqClient, err = NewSrvClient(nameServer, app1, s); err != nil {
		t.Fatal(err)
	}
	if c, err = NewClient(nameServer, app2); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	defer s.mqClient.Shutdown()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			si := strconv.Itoa(idx)
			msg := &Message{
				RemoteApp: app1,
				Tag:       tag1,
				Keys:      []string{si, getRandKey()},
				Body:      []byte("hello world " + si),
			}
			m, e := c.Request(msg)
			if e != nil {
				t.Errorf("rpc failed with msg: %s, keys: %s", msg.Body, msg.Keys)
				return
			}
			if bytes.Equal(m.Body, append(msg.Body, respSuffix...)) {
				t.Logf("rpc succeed with response: %s, keys: %s", m.Body, m.Keys)
			} else {
				t.Errorf("unexpected rpc response: %s", m.Body)
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(time.Second)
	t.Log("all test finished")
}

func TestConcurrentAsyncRPC(t *testing.T) {
	// 首先在rocketmq上新建topic：mqlib-test-1-rpc
	var (
		s   = newService() // RPC服务端
		c   PubClient      // RPC客户端
		n   = 10
		err error
	)
	if s.mqClient, err = NewSrvClient(nameServer, app1, s); err != nil {
		t.Fatal(err)
	}
	if c, err = NewClient(nameServer, app2); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	defer s.mqClient.Shutdown()
	for i := 0; i < n; i++ {
		si := strconv.Itoa(i)
		msg := &Message{
			RemoteApp: app1,
			Tag:       tag1,
			Keys:      []string{si, getRandKey()},
			Body:      []byte("hello world " + si),
		}
		err = c.RequestAsyncWithFunc(msg, func(m *Message) {
			if bytes.Equal(m.Body, append(msg.Body, respSuffix...)) {
				t.Logf("rpc succeed with response: %s, keys: %s", m.Body, m.Keys)
			} else {
				t.Errorf("unexpected rpc response: %s", m.Body)
			}
		})
		if err != nil {
			t.Errorf("rpc failed with msg: %s, keys: %s", msg.Body, msg.Keys)
		}
	}
	time.Sleep(time.Second * 6)
	t.Log("all test finished")
}

func TestOneLongRPC(t *testing.T) {
	// 首先在rocketmq上新建topic：mqlib-test-1-rpc
	var (
		s   = newService() // RPC服务端
		c   PubClient      // RPC客户端
		err error
	)
	bc, err := NewConsumer("test-broadcast-gp", nameServer, true, true, Topic{
		Name: pingTopic,
		Callback: func(msg *Message) error {
			log.Println("broadcast 1 info:", msg.ToString())
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	bc2, err := NewConsumer("test-broadcast-gp", nameServer, true, true, Topic{
		Name: pingTopic,
		Callback: func(msg *Message) error {
			log.Println("broadcast 2 info:", msg.ToString())
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.mqClient, err = NewSrvClient(nameServer, app1, s); err != nil {
		t.Fatal(err)
	}
	if c, err = NewClient(nameServer, app2, time.Hour); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	defer s.mqClient.Shutdown()
	defer bc2.Shutdown()
	defer bc.Shutdown()
	// sync
	msg := &Message{
		RemoteApp: app1,
		Tag:       tag1,
		Keys:      []string{getRandKey()},
		Body:      []byte("hello world 1"),
	}
	log.Print("rpc will finish in 20 seconds...")
	s.msgProTime = time.Second * 20 // 临时调整处理时间到20s
	defer func() {
		s.msgProTime = procTime
	}()
	m, err := c.Request(msg)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(m.Body, append(msg.Body, respSuffix...)) {
		t.Logf("rpc 1 succeed with response: %s, keys: %s", m.Body, m.Keys)
	} else {
		t.Errorf("unexpected rpc response: %s", m.Body)
	}
	t.Log("all test finished")
}
