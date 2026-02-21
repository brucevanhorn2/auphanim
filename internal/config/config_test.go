package config

import (
	"encoding/json"
	"os"
	"testing"
)

func TestInterpolateEnvVars(t *testing.T) {
	os.Setenv("TEST_HOST", "localhost")
	os.Setenv("TEST_PORT", "5432")
	defer os.Unsetenv("TEST_HOST")
	defer os.Unsetenv("TEST_PORT")

	input := []byte(`{"dsn": "${TEST_HOST}:${TEST_PORT}"}`)
	result, err := interpolateEnvVars(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"dsn": "localhost:5432"}`
	if string(result) != want {
		t.Errorf("got %q, want %q", string(result), want)
	}
}

func TestInterpolateEnvVars_MissingVar(t *testing.T) {
	os.Unsetenv("DEFINITELY_NOT_SET")
	input := []byte(`{"val": "${DEFINITELY_NOT_SET}"}`)
	_, err := interpolateEnvVars(input)
	if err == nil {
		t.Fatal("expected error for unset variable, got nil")
	}
}

func TestInterpolateEnvVars_SpecialChars(t *testing.T) {
	os.Setenv("SPECIAL", `has"quotes\and\\backslashes`)
	defer os.Unsetenv("SPECIAL")

	input := []byte(`{"val": "${SPECIAL}"}`)
	result, err := interpolateEnvVars(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed map[string]string
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Errorf("result is not valid JSON after interpolation: %v\nresult: %s", err, result)
	}
}

func TestLoad_UnknownWatcherType(t *testing.T) {
	f, err := os.CreateTemp("", "auphanim-test-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	f.WriteString(`{"watchers":[{"name":"x","type":"does-not-exist"}]}`)
	f.Close()

	cfg, err := Load(f.Name())
	// Load itself succeeds — type validation happens in the registry at startup.
	// We just want the config to parse correctly.
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if len(cfg.Watchers) != 1 || cfg.Watchers[0].Type != "does-not-exist" {
		t.Errorf("unexpected watchers: %+v", cfg.Watchers)
	}
}
