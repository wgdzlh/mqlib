package mqlib

import (
	"errors"
	"log"
	"math/rand"
	"strconv"
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
)

type SomeService struct {
	msgQue   chan *Message
	mqClient *Client
}

func newService() *SomeService {
	srv := &SomeService{
		msgQue: make(chan *Message, queSize),
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
	go func() {
		for msg := range s.msgQue {
			log.Printf("ss got message: %s", msg)
			switch msg.Tag {
			case tag1:
				time.Sleep(time.Second * 2)
				msg.Body = append(msg.Body, "---response"...)
				s.mqClient.Respond(msg)
			default:
				log.Print("unknown api tag:", msg.Tag)
				continue
			}
			log.Printf("ss finish message: %s", msg)
		}
	}()
}

func getRandKey() string {
	return strconv.FormatInt(rand.Int63n(10000)+10000, 10)
}

func TestAllRPC(t *testing.T) {
	// 首先在rocketmq上新建topic：mqlib-test-1-rpc
	var (
		s   = newService() // RPC服务端
		c   *Client        // RPC客户端
		err error
	)
	if s.mqClient, err = NewSrvClient(nameServer, app1, s); err != nil {
		t.Fatal(err)
	}
	if c, err = NewClient(nameServer, app2); err != nil {
		t.Fatal(err)
	}

	// sync
	msg := &Message{
		RemoteApp: app1,
		Tag:       tag1,
		Keys:      []string{getRandKey()},
		Body:      []byte("hello world 1"),
	}
	msg, err = c.Request(msg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rpc 1 succeed with response: %s, keys: %s", msg.Body, msg.Keys)

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
	msg = <-resChan
	t.Logf("rpc 2 succeed with response: %s, keys: %s", msg.Body, msg.Keys)

	// async func
	msg = &Message{
		RemoteApp: app1,
		Tag:       tag1,
		Keys:      []string{getRandKey()},
		Body:      []byte("hello world 3"),
	}
	if err = c.RequestAsyncWithFunc(msg, func(m *Message) {
		msg = m
		t.Logf("rpc 3 succeed with response: %s, keys: %s", msg.Body, msg.Keys)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 10)
}
