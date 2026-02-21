package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// WatcherConfig holds the parsed name/type for a watcher plus raw JSON so each
// watcher package can decode its own config schema without a central struct.
type WatcherConfig struct {
	Name   string
	Type   string
	Config json.RawMessage // the full watcher JSON object (including name/type)
}

// Config is the top-level application configuration.
type Config struct {
	Watchers []WatcherConfig
}

var envVarRe = regexp.MustCompile(`\$\{([A-Z0-9_]+)\}`)

// Load reads the config file at path (falling back to default search locations
// when path is empty), performs ${VAR} interpolation, and returns a Config.
func Load(path string) (*Config, error) {
	if path == "" {
		path = findConfigFile()
	}
	if path == "" {
		return nil, fmt.Errorf("no config file found; create ./auphanim.json or run with --init")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %q: %w", path, err)
	}

	data, err = interpolateEnvVars(data)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Watchers []json.RawMessage `json:"watchers"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg := &Config{}
	for _, rawWatcher := range raw.Watchers {
		var meta struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal(rawWatcher, &meta); err != nil {
			return nil, fmt.Errorf("parsing watcher entry: %w", err)
		}
		if meta.Name == "" {
			return nil, fmt.Errorf("watcher entry missing \"name\" field")
		}
		if meta.Type == "" {
			return nil, fmt.Errorf("watcher %q missing \"type\" field", meta.Name)
		}
		cfg.Watchers = append(cfg.Watchers, WatcherConfig{
			Name:   meta.Name,
			Type:   meta.Type,
			Config: rawWatcher,
		})
	}

	return cfg, nil
}

func findConfigFile() string {
	if _, err := os.Stat("./auphanim.json"); err == nil {
		return "./auphanim.json"
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	p := filepath.Join(home, ".config", "auphanim", "config.json")
	if _, err := os.Stat(p); err == nil {
		return p
	}
	return ""
}

// interpolateEnvVars replaces ${VAR_NAME} tokens in raw JSON bytes with the
// value of the corresponding environment variable. Fails fast if any variable
// is unset. Values are JSON-string-escaped so they are safe to embed in JSON.
func interpolateEnvVars(data []byte) ([]byte, error) {
	var lastErr error
	result := envVarRe.ReplaceAllFunc(data, func(match []byte) []byte {
		if lastErr != nil {
			return match
		}
		varName := string(envVarRe.FindSubmatch(match)[1])
		val, ok := os.LookupEnv(varName)
		if !ok {
			lastErr = fmt.Errorf("environment variable %q is not set (referenced in config)", varName)
			return match
		}
		// JSON-encode the value so characters like " and \ are safely escaped.
		encoded, _ := json.Marshal(val)
		// Marshal wraps in quotes; strip them since we're already inside a JSON string.
		return encoded[1 : len(encoded)-1]
	})
	return result, lastErr
}
