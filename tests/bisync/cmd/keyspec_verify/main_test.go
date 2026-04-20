package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSampleMatchesTags(t *testing.T) {
	t.Parallel()

	sample := sampleCase{
		name: "json-set",
		tags: []string{"module", "module-json"},
	}
	if !sampleMatchesTags(sample, []string{"module"}) {
		t.Fatalf("expected module tag to match")
	}
	if sampleMatchesTags(sample, []string{"core"}) {
		t.Fatalf("did not expect core tag to match")
	}
}

func TestLoadSamplesIncludesBuiltinsAndExternalFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "extra.json")
	data := `[{"name":"custom-json","cmd":"json.numincrby","args":["doc{t}","$","1"],"tags":["module","module-json"]}]`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write sample file failed: %v", err)
	}

	samples, err := loadSamples(path, []string{"module-json"})
	if err != nil {
		t.Fatalf("loadSamples failed: %v", err)
	}
	if len(samples) == 0 {
		t.Fatalf("expected filtered samples")
	}

	foundBuiltin := false
	foundExternal := false
	for _, sample := range samples {
		switch sample.name {
		case "json-set":
			foundBuiltin = true
		case "custom-json":
			foundExternal = true
		}
	}
	if !foundBuiltin {
		t.Fatalf("expected builtin module-json sample to remain after filtering")
	}
	if !foundExternal {
		t.Fatalf("expected external module-json sample to be loaded")
	}
}

func TestReadSamplesFileRejectsMissingName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "broken.json")
	if err := os.WriteFile(path, []byte(`[{"cmd":"set","args":["k","v"]}]`), 0o644); err != nil {
		t.Fatalf("write sample file failed: %v", err)
	}

	if _, err := readSamplesFile(path); err == nil {
		t.Fatalf("expected invalid sample file to fail")
	}
}
