package q

import (
	"testing"
	"time"
)

func TestValidName(t *testing.T) {
	if !IsValidRequestName("a") {
		t.Errorf("a was not a valid request name")
	}
	if !IsValidRequestName("a.b") {
		t.Errorf("a.b was not a valid request name")
	}
	if !IsValidRequestName("123") {
		t.Errorf("123 was not a valid request name")
	}
}
func TestInvalidName(t *testing.T) {
	if IsValidRequestName("a-b") {
		t.Errorf("a-b was a valid request name")
	}
	if IsValidRequestName("a.*") {
		t.Errorf("a.* was a valid request name")
	}
	if IsValidRequestName("a.>") {
		t.Errorf("a.> was a valid request name")
	}
}

func TestSend(t *testing.T) {
	err := Send("", "test.message", []byte("test message"))
	if err != nil {
		t.Error(err)
	}
}

func TestInvalidSend(t *testing.T) {
	err := Send("", "test-message", []byte("test message"))
	if err == nil {
		t.Error("Expected Send to return an error")
	}
}

func TestSendEmpty(t *testing.T) {
	err := Send("", "test.message", []byte{})
	if err != nil {
		t.Errorf("Send {} got %s", err)
	}
	err = Send("", "test.message", []byte(nil))
	if err != nil {
		t.Errorf("Send nil got %s", err)
	}
}

func TestParseMessage(t *testing.T) {
	var msg = []byte("test:first\nname:blue\n\nmessage")
	h, m := ParseMessage(msg)
	if string(m) != "message" {
		t.Errorf("Expected message got %s", m)
	}
	if _, ok := h["test"]; !ok {
		t.Error("test was not found in header")
	}
	if value, _ := h["test"]; value != "first" {
		t.Errorf("Expected test to be first, got %s", value)
	}
	if _, ok := h["name"]; !ok {
		t.Error("name was not found in header")
	}
	if value, _ := h["name"]; value != "blue" {
		t.Errorf("Expected name to be blue, got %s", value)
	}
}

func TestBuildMessage(t *testing.T) {
	var headers = []Header{{Key: "test", Value: "first"}, {Key: "name", Value: "blue"}}

	msg := buildMessage("", []byte("message"), headers...)

	h, m := ParseMessage(msg)
	if string(m) != "message" {
		t.Errorf("Expected message got %s", m)
	}
	if _, ok := h["test"]; !ok {
		t.Error("test was not found in header")
	}
	if value, _ := h["test"]; value != "first" {
		t.Errorf("Expected test to be first, got %s", value)
	}
	if _, ok := h["name"]; !ok {
		t.Error("name was not found in header")
	}
	if value, _ := h["name"]; value != "blue" {
		t.Errorf("Expected name to be blue, got %s", value)
	}
	if _, ok := h["traceId"]; !ok {
		t.Error("traceId was not found in header")
	}
}

func TestFailedRequest(t *testing.T) {
	_, err := Request("", "test.message", []byte("message"), 100*time.Millisecond)
	if err == nil {
		t.Errorf("Request expected error got nothing")
	}
}
