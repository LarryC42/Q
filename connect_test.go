package q

import (
	"testing"
)

func TestConnectTo(t *testing.T) {
	_, err := Open(ConnectTo("nats://127.0.0.1:4222"))
	if err != nil {
		t.Error(err)
	}
	CloseAll()
}

func TestSetAppName(t *testing.T) {
	_, err := Open(SetAppName("Blue"))
	if err != nil {
		t.Error(err)
	}
	if AppName() != "Blue" {
		t.Errorf("AppName not set, expected Blue got %s", AppName())
	}
	CloseAll()
}

// Timeout - Doesn't timeout in 1ns, leave it be

func TestIsOpen(t *testing.T) {
	_, err := Open()
	if err != nil {
		t.Error(err)
	}
	if !IsOpen() {
		t.Error("Expected IsOpen to be true")
	}
	CloseAll()
}

func TestIsOpenClosed(t *testing.T) {
	if IsOpen() {
		t.Error("Expected IsOpen to return false")
	}
}
