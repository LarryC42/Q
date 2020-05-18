package q

import (
	"errors"
	"fmt"
	"log"
	"unicode"

	nats "github.com/nats-io/nats.go"
)

// NoPrivateSubscription turns off the private subscription for the server
func NoPrivateSubscription() Option {
	return func(t *Options) {
		t.privateSubs = false
	}
}

// InitialScale sets the initial scale option to n which must be > 0, default is 1
func InitialScale(n int) Option {
	if n < 1 {
		log.Fatal(fmt.Sprintf("scale '%d' is not valid, must be >0", n))
	}
	return func(t *Options) {
		t.scale = n
	}
}

// AutoUnsubscribe will autounsubscribe after n messages received
func AutoUnsubscribe(n int) Option {
	if n < 1 {
		log.Fatal(fmt.Sprintf("AutoUnsubscribe '%d' is not valid, must be >0", n))
	}
	return func(t *Options) {
		t.unsubscribe = n
	}
}

// Handler is the definition of a routine called when a server is invoked
type Handler func(Server, string, []byte) ([]byte, error)

// Server handles scaling a server
type Server interface {
	Scale(n int) error
	Count() int
	Close() error
	ID() string
}

type server struct {
	id           string
	subscription *nats.Subscription
	privatesubs  *nats.Subscription
	topic        string
	queue        string
	handler      Handler
	options      *Options
}

// NewTopic returns a new topic server
func NewTopic(topic string, handler Handler, opts ...Option) (Server, error) {
	return newServer(topic, "", handler, opts...)
}

// NewQueue returns a new queue server
func NewQueue(topic, queue string, handler Handler, opts ...Option) (Server, error) {
	return newServer(topic, queue, handler, opts...)
}

// Scale scales the active servers up or down by n
func (s server) Scale(n int) error {
	return Scale(s.topic, n)
}

// Count returns the number of active servers
func (s server) Count() int {
	return Count(s.topic)
}

// Close closes all active servers
func (s server) Close() error {
	return Close(s.topic)
}

// ID returns the servers unique id
func (s server) ID() string {
	return s.id
}

var subscriptions map[string][]*server = make(map[string][]*server)

// IsValidServerName returns true if topic is a valid subscription
func IsValidServerName(name string) bool {
	gt := false
	if len(name) == 0 {
		return false
	}
	for _, r := range name {
		if gt {
			return false // Something after >
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != rune('.') && r != rune('*') && r != rune('>') {
			return false // Not a valid character
		}
		if r == rune('>') {
			gt = true // Track we hit the end
		}
	}
	return true
}

// IsServerAvailable returns true if a topic is available
func IsServerAvailable(topic string) bool {
	_, ok := subscriptions[topic]
	return !ok
}

func newServer(serverName, queueName string, handler Handler, opts ...Option) (Server, error) {
	var err error
	if !IsValidServerName(serverName) {
		return nil, fmt.Errorf("server '%s' is an invalid name", serverName)
	}
	if !IsServerAvailable(serverName) {
		return nil, fmt.Errorf("server '%s' already exists", serverName)
	}
	options, err := Open(opts...)
	if err != nil {
		return nil, errors.New("No NATS")
	}

	svc, err := new(serverName, queueName, handler, options)
	if err != nil {
		return nil, err
	}
	subscriptions[serverName] = []*server{}
	subscriptions[serverName] = append(subscriptions[serverName], svc)
	if options.scale > 1 {
		err = Scale(serverName, options.scale-1)
		if err != nil {
			return svc, err
		}
	}
	return svc, nil
}

func new(serverName, queue string, handler Handler, opt *Options) (*server, error) {
	var err error
	svc := server{}
	svc.id = NewID()
	svc.handler = handler
	svc.topic = serverName
	svc.queue = queue
	svc.options = opt

	if queue == "" {
		svc.subscription, err = nc.Subscribe(svc.topic, func(m *nats.Msg) {
			reply, err := svc.handler(svc, m.Subject, m.Data)
			if err != nil {
				nc.Publish(m.Reply, []byte(fmt.Sprintf("error:%s", err)))
			}
			if m.Reply != "" {
				nc.Publish(m.Reply, reply)
			}
		})
	} else {
		svc.subscription, err = nc.QueueSubscribe(svc.topic, svc.queue, func(m *nats.Msg) {
			reply, err := svc.handler(svc, m.Subject, m.Data)
			if err != nil {
				nc.Publish(m.Reply, []byte(fmt.Sprintf("error:%s", err)))
			}
			if m.Reply != "" {
				nc.Publish(m.Reply, reply)
			}
		})
	}
	if err != nil {
		return nil, err
	}
	if opt.privateSubs {
		svc.privatesubs, err = nc.Subscribe(svc.id, func(m *nats.Msg) {
			reply, err := svc.handler(svc, m.Subject, m.Data)
			if err != nil {
				nc.Publish(m.Reply, []byte("error"))
			}
			if m.Reply != "" && reply != nil {
				nc.Publish(m.Reply, reply)
			}
		})
	}
	return &svc, nil
}

func scaleUp(topic string, n int) error {
	services, ok := subscriptions[topic]
	if !ok || len(services) == 0 {
		return fmt.Errorf("server '%s' was not found", topic)
	}
	s := services[0]

	for i := 0; i < n; i++ {
		svc, err := new(s.topic, s.queue, s.handler, s.options)
		if err != nil {
			return err
		}
		services = append(services, svc)
	}
	subscriptions[topic] = services
	return nil
}

func scaleDown(topic string, n int) error {
	services, ok := subscriptions[topic]
	if !ok || len(services) == 0 {
		return fmt.Errorf("server '%s' was not found", topic)
	}

	for i := 0; i < n; i++ {
		if len(services) == 0 {
			break
		}
		s := services[len(services)-1]
		s.subscription.Unsubscribe()
		s.privatesubs.Unsubscribe()
		services = services[:len(services)-1]
	}
	if len(services) == 0 {
		delete(subscriptions, topic)
	} else {
		subscriptions[topic] = services
	}
	if len(subscriptions) == 0 {
		close()
	}
	return nil
}

// Scale scales the number of servers by n
func Scale(topic string, n int) error {
	if n > 0 {
		return scaleUp(topic, n)
	} else if n < 0 {
		return scaleDown(topic, -n)
	}
	return nil // nothing to do if n==0, also not an error
}

// Count returns the number of active servers for a topic
func Count(topic string) int {
	services, ok := subscriptions[topic]
	if !ok {
		return 0
	}
	return len(services)
}
