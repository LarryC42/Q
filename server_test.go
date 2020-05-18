package q

import "testing"

func TestInvalidTopic(t *testing.T) {
	_, err := NewTopic("bad-topic", func(svr Server, topic string, message []byte) ([]byte, error) { return nil, nil })
	if err == nil {
		t.Error("Invalid topic expected error")
	}
}

func TestInvalidQueue(t *testing.T) {
	_, err := NewQueue("bad-topic", "queue", func(svr Server, topic string, message []byte) ([]byte, error) { return nil, nil })
	if err == nil {
		t.Error("Invalid topic expected error")
	}
}

func TestNoDupicate(t *testing.T) {
	_, err := NewQueue("test.no.dups", "queue", func(svr Server, topic string, message []byte) ([]byte, error) { return nil, nil })
	defer Close("test.no.dups")
	if err != nil {
		t.Errorf("New queue got %s", err)
	}
	_, err = NewQueue("test.no.dups", "queue", func(svr Server, topic string, message []byte) ([]byte, error) { return nil, nil })
	if err == nil {
		t.Errorf("Duplicate queue generated no error")
	}
}

func TestScale(t *testing.T) {
	s, err := NewQueue("test.scale", "queue", func(svr Server, topic string, message []byte) ([]byte, error) { return nil, nil })
	defer Close("test.scale")
	if err != nil {
		t.Errorf("Valid topic returned %s", err)
	}
	if s.Count() != 1 {
		t.Errorf("Expected 1 server, got %d", s.Count())
	}
	s.Scale(1)
	if s.Count() != 2 {
		t.Errorf("Expected scale up to 2 servers, got %d", s.Count())
	}
	s.Scale(-1)
	if s.Count() != 1 {
		t.Errorf("Expected scale down to 1 server, got %d", s.Count())
	}
	s.Scale(-1)
	if Count("test.message") != 0 {
		t.Errorf("Expected scale down to 0 server, got %d", Count("test.message"))
	}
}
