package q

import (
	"errors"
	"fmt"
	"math/rand"
)

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
