package q

import (
	"fmt"
	"testing"
)

// TestAppName tests the AppName function
func TestAppName(t *testing.T) {
	var name = AppName()
	if name != "q.test" {
		t.Errorf("Got %s expected q.test", name)
	}
}

func TestAppID(t *testing.T) {
	var id = AppID()
	fmt.Println(id)
}
