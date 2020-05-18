package q

import (
	"errors"
	"time"

	nats "github.com/nats-io/nats.go"
)

var nc *nats.Conn
var defaultOptions *Options = newOptions()

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

func newOptions() *Options {
	return &Options{
		connect:     nats.DefaultURL,
		name:        AppName(),
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

func close() {
	if nc != nil {
		nc.Flush()
		nc.Close()
		nc = nil
	}
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
