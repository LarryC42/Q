package q

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// NewID returns a new id
func NewID() string {
	return fmt.Sprintf("q%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

// BaseFilename returns the file name without path or extension
func BaseFilename(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, filepath.Ext(filename)), filepath.Dir(filename))[1:]
}

// Input display a message and then captures a line of input
func Input(message string) string {
	fmt.Print(message)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return scanner.Text()
}
