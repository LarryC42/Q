package q

import (
	"strings"
	"testing"
	"unicode"
)

func TestNewIDNoDashes(t *testing.T) {
	var id = NewID()
	if strings.Index(id, "-") > -1 {
		t.Error("ID contains -'s")
	}
}

func TestIDStartsAlpha(t *testing.T) {
	var id = NewID()
	if !unicode.IsLetter(rune(id[0])) {
		t.Error("ID doesn't start with alpha")
	}
}

func TestBaseFilenameNoBs(t *testing.T) {
	var name = "c:\\a\\b\\c\\file.ext"
	var base = BaseFilename(name)
	if strings.Index(base, "\\") > -1 {
		t.Errorf("BaseFilename contains \\'s, %s=%s", name, base)
	}
}

func TestBaseFilenameNoColon(t *testing.T) {
	var name = "c:\\a\\b\\c\\file.ext"
	var base = BaseFilename(name)
	if strings.Index(base, ":") > -1 {
		t.Errorf("BaseFilename contains :'s, %s=%s", name, base)
	}
}

func TestBaseFilenameNoExt(t *testing.T) {
	var name = "c:\\a\\b\\c\\file.ext"
	var base = BaseFilename(name)
	if strings.Index(base, ".") > -1 {
		t.Errorf("BaseFilename contains an extension %s=%s", name, base)
	}
}

func TestBaseFilenameValue(t *testing.T) {
	var name = "c:\\a\\b\\c\\file.ext"
	var base = BaseFilename(name)
	if base != "file" {
		t.Errorf("BaseFilename invalid %s=%s", name, base)
	}
}
