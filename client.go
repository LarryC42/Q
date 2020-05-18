package q

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Header is a key/value pair to prepend to messages
type Header struct {
	Key   string
	Value string
}

// IsValidRequestName returns true if topic is a valid topic
func IsValidRequestName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != rune('.') {
			return false
		}
	}
	return true
}

// Send sends a message to serverName
func Send(traceID, serverName string, message []byte, headers ...Header) error {
	if !IsValidRequestName((serverName)) {
		return fmt.Errorf("'%s' was not a valid request name", serverName)
	}
	if nc == nil {
		Open()
	}
	if traceID == "" {
		traceID = NewID()
	}
	return nc.Publish(serverName, buildMessage(traceID, message, headers...))
}

// Request sends a request to serverName and returns reply
func Request(traceID, serverName string, message []byte, timeout time.Duration, headers ...Header) ([]byte, error) {
	if !IsValidRequestName((serverName)) {
		return nil, fmt.Errorf("'%s' was not a valid request name", serverName)
	}
	if nc == nil {
		Open()
	}
	if traceID == "" {
		traceID = NewID()
	}
	reply, err := nc.Request(serverName, buildMessage(traceID, message, headers...), timeout)
	if err != nil {
		return nil, err
	}
	var msg = string(reply.Data)
	if msg[:6] == "error:" {
		return nil, errors.New(msg[6:])
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
