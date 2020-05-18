package q

import (
	"errors"
	"fmt"
	"math/rand"
)

// HelloServer is an example server handler
func HelloServer(svr Server, topic string, message []byte) ([]byte, error) {
	// Sample output
	// [App: Blue, AppId: q94db4cd4648c48279fc653675326abce, Topic: test.hello, Trace tid] Hello Bob from q1b59eba51e9c42c29aa228716ae72486
	mp, msg := ParseMessage(message)
	return []byte(fmt.Sprintf("[App: %s, AppId: %s, Topic: %s, Trace %s] Hello %s from %s", mp["appName"], mp["appId"], topic, mp["traceId"], string(msg), svr.ID())), nil
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

// MatchFunc determines which Handler to select
type MatchFunc func(int, Server, string, []byte) int

// RandomSelection selects from m servers randomly
func RandomSelection() MatchFunc {
	return func(n int, _ Server, _ string, _ []byte) int {
		return rand.Intn(n)
	}
}

// RouteServer uses selector to select one of the servers.  Can be used for A/B testing, filtering, routing
func RouteServer(selector MatchFunc, servers ...Handler) Handler {
	return func(svr Server, topic string, message []byte) ([]byte, error) {
		index := selector(len(servers), svr, topic, message)
		if index < 0 || index > len(servers) {
			return nil, fmt.Errorf("selected server %d was not in the range [0..%d)", index, len(servers))
		}
		if servers[index] == nil {
			return nil, nil
		}
		return servers[index](svr, topic, message)
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
