package migration

import (
	"strings"
	"testing"
	"testing/fstest"
)

// TestScripts validates version ordering and stable checksums across checkout line endings.
func TestScripts(t *testing.T) {
	sql := "BEGIN;\nSELECT 1;\nCOMMIT;\n"
	makeFS := func(body string) fstest.MapFS {
		return fstest.MapFS{"000001_first.up.sql": {Data: []byte(body)}}
	}
	lf, err := New(makeFS(sql))
	if err != nil {
		t.Fatal(err)
	}
	crlf, err := New(makeFS(strings.ReplaceAll(sql, "\n", "\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if lf.scripts[0].checksum != crlf.scripts[0].checksum {
		t.Fatal("platform-dependent checksum")
	}
	invalid := []fstest.MapFS{
		{},
		{"000002_gap.up.sql": {Data: []byte(sql)}},
		makeFS("SELECT 1;"),
		{"bad.up.sql": {Data: []byte(sql)}},
	}
	for _, files := range invalid {
		if _, err := New(files); err == nil {
			t.Fatal("invalid script set accepted")
		}
	}
}
