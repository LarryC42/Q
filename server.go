package q

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	nats "github.com/nats-io/nats.go"
)

var nc *nats.Conn
var defaultOptions *Options = newOptions()
var appName string = BaseFilename(os.Args[0])
var appID string = uuid.New().String()

// BaseFilename returns the file name without path or extension
func BaseFilename(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, filepath.Ext(filename)), filepath.Dir(filename))
}

// AppID is the unique ID of this instance of the application
func AppID() string {
	return appID
}

// AppName returns the name of this application
func AppName() string {
	return appName
}

// Options is the internal current set of options that can be overridden
type Options struct {
	connect     string
	name        string
	timeout     time.Duration
	privateSubs bool
	scale       int
	unsubscribe int
}

// Option is a function definition for extensible options
type Option func(*Options)

// ConnectTo sets the url to connect to
func ConnectTo(url string) Option {
	return func(t *Options) {
		t.connect = url
	}
}

// SetAppName sets the application name
func SetAppName(name string) Option {
	return func(t *Options) {
		appName = name
	}
}

// Name sets the name of the client, default is Q
func Name(name string) Option {
	return func(t *Options) {
		t.name = name
	}
}

// Timeout sets the timeout for connecting, default is 100ms
func Timeout(value time.Duration) Option {
	return func(t *Options) {
		t.timeout = value
	}
}

// NoPrivateSubscription turns off the private subscription for the server
func NoPrivateSubscription() Option {
	return func(t *Options) {
		t.privateSubs = false
	}
}

// InitialScale sets the initial scale to n which must be > 0, default is 1
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

func newOptions() *Options {
	return &Options{
		connect:     nats.DefaultURL,
		name:        "Q",
		timeout:     100 * time.Millisecond,
		privateSubs: true,
		scale:       1,
		unsubscribe: -1,
	}
}

// SetDefaultOptions sets the default options to use
func SetDefaultOptions(opts ...Option) {
	var def = newOptions()
	for _, opt := range opts {
		opt(def)
	}
	defaultOptions = def
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
	return newSubscription(topic, "", handler, opts...)
}

// NewQueue returns a new queue server
func NewQueue(topic, queue string, handler Handler, opts ...Option) (Server, error) {
	return newSubscription(topic, queue, handler, opts...)
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

var subscriptions map[string][]*server

// IsValidPublication returns true if topic is a valid topic
func IsValidPublication(topic string) bool {
	if len(topic) == 0 {
		return false
	}
	for _, r := range topic {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != rune('.') {
			return false
		}
	}
	return true
}

// IsValidSubscription returns true if topic is a valid subscription
func IsValidSubscription(topic string) bool {
	gt := false
	if len(topic) == 0 {
		return false
	}
	for _, r := range topic {
		if gt {
			return false
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != rune('.') && r != rune('*') && r != rune('>') {
			return false
		}
		if r == rune('>') {
			gt = true
		}
	}
	return true
}

// IsSubscriptionAvailable returns true if a topic is available
func IsSubscriptionAvailable(topic string) bool {
	_, ok := subscriptions[topic]
	return !ok
}

// IsOpen returns true if ready to send messages
func IsOpen() bool {
	return nc != nil
}

// Open initiates the ability to send messages
func Open(opts ...Option) (*Options, error) {
	if nc == nil {
		var err error
		copyOptions := *defaultOptions
		options := &copyOptions
		for _, o := range opts {
			o(options)
		}
		nc, err = nats.Connect(options.connect, nats.Name(options.name), nats.Timeout(options.timeout))
		if err != nil {
			return nil, errors.New("No NATS")
		}
		return options, nil
	}
	return defaultOptions, nil
}
func newSubscription(topic, queue string, handler Handler, opts ...Option) (Server, error) {
	var err error
	if !IsValidSubscription(topic) {
		return nil, fmt.Errorf("server '%s' is an invalid name", topic)
	}
	if !IsSubscriptionAvailable(topic) {
		return nil, fmt.Errorf("server '%s' already exists", topic)
	}
	options, err := Open(opts...)
	if err != nil {
		return nil, errors.New("No NATS")
	}

	svc, err := new(topic, queue, handler, options)
	if err != nil {
		return nil, err
	}
	subscriptions[topic] = []*server{}
	subscriptions[topic] = append(subscriptions[topic], svc)
	if options.scale > 1 {
		err = Scale(topic, options.scale-1)
		if err != nil {
			return svc, err
		}
	}
	return svc, nil
}

func new(topic, queue string, handler Handler, opt *Options) (*server, error) {
	var err error
	svc := server{}
	svc.id = uuid.New().String()
	svc.handler = handler
	svc.topic = topic
	svc.queue = queue
	svc.options = opt

	if queue == "" {
		svc.subscription, err = nc.Subscribe(svc.topic, func(m *nats.Msg) {
			reply, err := svc.handler(svc, m.Subject, m.Data)
			if err != nil {
				nc.Publish(m.Reply, []byte("error"))
			}
			if m.Reply != "" {
				nc.Publish(m.Reply, reply)
			}
		})
	} else {
		svc.subscription, err = nc.QueueSubscribe(svc.topic, svc.queue, func(m *nats.Msg) {
			reply, err := svc.handler(svc, m.Subject, m.Data)
			if err != nil {
				nc.Publish(m.Reply, []byte("error"))
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

func close() {
	if nc != nil {
		nc.Flush()
		nc.Close()
		nc = nil
	}
}

// Scale scales the number of servers by n
func Scale(topic string, n int) error {
	if n > 0 {
		scaleUp(topic, n)
	} else if n < 0 {
		scaleDown(topic, -n)
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

// CloseAll closes everything
func CloseAll() error {
	keys := make([]string, len(subscriptions))

	i := 0
	for k := range subscriptions {
		keys[i] = k
		i++
	}
	for _, k := range keys {
		err := Close(k)
		if err != nil {
			return err
		}

	}
	close()
	return nil
}

// Close reduces a topics servers to 0
func Close(topic string) error {
	if len(subscriptions) == 0 {
		close()
		return nil
	}
	return scaleDown(topic, Count(topic))
}

// Input display a message and then captures a line of input
func Input(message string) string {
	fmt.Print(message)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return scanner.Text()
}

// HelloServer is an example server handler
func HelloServer(svr Server, topic string, message []byte) ([]byte, error) {
	return []byte(fmt.Sprintf("[Topic: %s] Hello %s from %s", topic, string(message), svr.ID())), nil
}

// IgnoreServer will ignore n of m requests and return errMsg as the error, is an example wrapper of server handlers to add additional functionality
func IgnoreServer(server Handler, n, m int, errMsg string) Handler {
	return func(svr Server, topic string, message []byte) ([]byte, error) {
		if rand.Intn(m) < n {
			return nil, errors.New(errMsg)
		}
		return server(svr, topic, message)
	}
}

// AbServer will call server aServer n out of m times.  Otherwise calls bServer
func AbServer(aServer, bServer Handler, n, m int) Handler {
	return func(svr Server, topic string, message []byte) ([]byte, error) {
		if rand.Intn(m) < n {
			return aServer(svr, topic, message)
		}
		return bServer(svr, topic, message)
	}
}

// FallbackServer calls fallbackServer if main server returns an error
func FallbackServer(server, fallbackServer Handler) Handler {
	return func(svr Server, topic string, message []byte) ([]byte, error) {
		b, e := server(svr, topic, message)
		if e != nil {
			b, e = fallbackServer(svr, topic, message)
		}
		return b, e
	}
}

// FilterServer tests if the server should be called, returns nothing if excluded
func FilterServer(server Handler, include func(Server, string, []byte) bool) Handler {
	return func(svr Server, topic string, message []byte) ([]byte, error) {
		if include(svr, topic, message) {
			return server(svr, topic, message)
		}
		return nil, nil
	}
}

// -----------------------------------------------------------------------------
// Client
// -----------------------------------------------------------------------------

// Header is a key/value pair to prepend to messages
type Header struct {
	Key   string
	Value string
}

// Publish sends a message
func Publish(traceID, serverName string, message []byte, headers ...Header) error {
	if nc == nil {
		Open()
	}
	if traceID == "" {
		traceID = uuid.New().String()
	}
	return nc.Publish(serverName, buildMessage(traceID, message, headers...))
}

// Request sends a request and returns reply
func Request(traceID, serverName string, message []byte, timeout time.Duration, headers ...Header) ([]byte, error) {
	if nc == nil {
		Open()
	}
	if traceID == "" {
		traceID = uuid.New().String()
	}
	reply, err := nc.Request(serverName, buildMessage(traceID, message, headers...), timeout)
	if err != nil {
		return nil, err
	}
	return reply.Data, nil
}

// ParseMessage breaks up message into headers and message string
func ParseMessage(message []byte) (map[string]string, []byte) {
	var headers = make(map[string]string)

	i := bytes.Index(message, []byte{'\n', '\n'})
	if i < 0 {
		return headers, nil
	}
	header := string(message[:i])
	ret := message[i+2:]
	pairs := strings.Split(header, "\n")
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) == 2 {
			headers[parts[0]] = parts[1]
		}
	}
	return headers, ret
}

func buildMessage(traceID string, message []byte, headers ...Header) []byte {
	var sb strings.Builder
	sb.WriteString("traceId:")
	sb.WriteString(traceID)
	sb.WriteString("\nappId:")
	sb.WriteString(appID)
	sb.WriteString("\nappName:")
	sb.WriteString(appName)
	for _, header := range headers {
		sb.WriteString("\n")
		sb.WriteString(header.Key)
		sb.WriteString(":")
		sb.WriteString(header.Value)
	}
	sb.WriteString("\n\n")
	return append([]byte(sb.String()), message...)
}
