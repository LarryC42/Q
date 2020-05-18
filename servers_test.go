package q

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func SimpleServer(svr Server, topic string, message []byte) ([]byte, error) {
	return []byte("Hello"), nil
}

func SimpleA(svr Server, topic string, message []byte) ([]byte, error) {
	return []byte("a"), nil
}
func SimpleB(svr Server, topic string, message []byte) ([]byte, error) {
	return []byte("b"), nil
}
func Bad(svr Server, topic string, message []byte) ([]byte, error) {
	return nil, errors.New("oops")
}
func Good(svr Server, topic string, message []byte) ([]byte, error) {
	return []byte("good"), nil
}
func Fallback(svr Server, topic string, message []byte) ([]byte, error) {
	return []byte("fallback"), nil
}

func TestSimple(t *testing.T) {
	svr, err := NewTopic("test.simple", SimpleServer)
	if err != nil {
		t.Errorf("TestSimple NewTopic got %s", err)
	}
	defer svr.Close()

	reply, err := Request("rid", "test.simple", []byte{}, 100*time.Millisecond)
	if err != nil {
		t.Errorf("TestSimple Reply got %s", err)
	}
	if string(reply) != "Hello" {
		t.Errorf("TestSimple Request expected Hello got %s", string(reply))
	}
}

func TestHelloServer(t *testing.T) {
	// Sample output
	// [App: Blue, AppId: q94db4cd4648c48279fc653675326abce, Topic: test.hello, Trace tid] Hello Bob from q1b59eba51e9c42c29aa228716ae72486
	svr, err := NewTopic("test.hello", HelloServer)
	if err != nil {
		t.Errorf("TestHelloServer NewTopic got %s", err)
	}
	defer svr.Close()
	reply, err := Request("tid", "test.hello", []byte("Bob"), 100*time.Millisecond)
	if err != nil {
		t.Errorf("TestHelloServer Reply got %s", err)
	}
	msg := string(reply)
	if strings.Index(msg, "App: Blue") < 0 {
		t.Error("TestHelloServer did not find App: Blue in reply")
	}
	if strings.Index(msg, "AppId: ") < 0 {
		t.Error("TestHelloServer did not find AppId: in reply")
	}
	if strings.Index(msg, "Topic: test.hello,") < 0 {
		t.Error("TestHelloServer did not find Topic: test.hello in reply")
	}
	if strings.Index(msg, "Trace tid") < 0 {
		t.Error("TestHelloServer did not find Trace tid in reply")
	}
	if strings.Index(msg, "Hello Bob from ") < 0 {
		t.Error("TestHelloServer did not find Hello Bob from in reply")
	}
}

func TestRandom(t *testing.T) {
	svr, err := NewTopic("test.random", RouteServer(RandomSelection(), SimpleA, SimpleB))
	if err != nil {
		t.Errorf("TestRandom NewTopic got %s", err)
	}
	defer svr.Close()

	results := make(map[string]int)
	for i := 0; i < 20; i++ {
		reply, err := Request("rid", "test.random", []byte{}, 100*time.Millisecond)
		if err != nil {
			t.Errorf("TestRandom Reply got %s", err)
		}
		results[string(reply)]++
	}
	if results["a"] == 0 || results["b"] == 0 {
		t.Errorf("TestRandom expected calls to a got (%d) and b got (%d)", results["a"], results["b"])
	}
}
func TestIgnore(t *testing.T) {
	svr, err := NewTopic("test.ignore", IgnoreServer(SimpleA, 1, 2, "talk to the hand"))
	if err != nil {
		t.Errorf("TestIgnore NewTopic got %s", err)
	}
	defer svr.Close()

	var errCount int

	for i := 0; i < 20; i++ {
		_, err := Request("rid", "test.ignore", []byte{}, 100*time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	if errCount == 0 || errCount == 20 {
		t.Errorf("TestIgnore expected ~10 errors, got %d", errCount)
	}
}

func TestFallbackBad(t *testing.T) {
	svr, err := NewTopic("test.fallback.bad", FallbackServer(Bad, Fallback))
	if err != nil {
		t.Errorf("TestFallbackBad NewTopic got %s", err)
	}
	defer svr.Close()

	reply, err := Request("rid", "test.fallback.bad", []byte{}, 100*time.Millisecond)
	if err != nil {
		t.Errorf("TestFallbackBad got error %s", err)
	}
	if string(reply) != "fallback" {
		t.Errorf("TestFallbackBad expected fallback got %s", string(reply))
	}
}

func TestFallbackGood(t *testing.T) {
	svr, err := NewTopic("test.fallback.good", FallbackServer(Good, Fallback))
	if err != nil {
		t.Errorf("TestFallbackGood NewTopic got %s", err)
	}
	defer svr.Close()

	reply, err := Request("rid", "test.fallback.good", []byte{}, 100*time.Millisecond)
	if err != nil {
		t.Errorf("TestFallbackGood got error %s", err)
	}
	if string(reply) != "good" {
		t.Errorf("TestFallbackGood expected good got %s", string(reply))
	}
}
