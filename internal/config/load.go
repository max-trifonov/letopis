package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// FileName is what we look for when no explicit path is given.
const FileName = "config.yaml"

// Resolve picks the config file to use: the explicit path if given,
// otherwise FileName in the working directory, otherwise next to the
// binary. An explicit path that doesn't exist is an error; a missing
// default is too — the service does not run on built-ins alone, because
// a silently default-configured instance is worse than a refused start.
func Resolve(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("config file %s: %w", explicit, err)
		}
		return explicit, nil
	}
	candidates := []string{FileName}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), FileName))
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", errors.New("no config file found: put config.yaml next to the binary or in the working directory, or pass --config")
}

// Load reads the file over Default(), applies environment overrides and
// validates the result.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := applyEnv(cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// applyEnv lets deployments override the file without editing it.
// The set is deliberately small; secrets are the usual reason to prefer env
// vars over the file.
func applyEnv(cfg *Config) error {
	setStr := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok {
			*dst = v
		}
	}
	if v, ok := os.LookupEnv("LETOPIS_ROLE"); ok {
		cfg.Role = Role(v)
	}
	setStr("LETOPIS_HTTP_ADDR", &cfg.Server.HTTP.Addr)
	setStr("LETOPIS_GRPC_ADDR", &cfg.Server.GRPC.Addr)
	setStr("LETOPIS_MONGODB_URI", &cfg.MongoDB.URI)
	setStr("LETOPIS_REDIS_ADDR", &cfg.Redis.Addr)
	setStr("LETOPIS_REDIS_PASSWORD", &cfg.Redis.Password)
	setStr("LETOPIS_LOG_LEVEL", &cfg.Log.Level)
	if v, ok := os.LookupEnv("LETOPIS_REDIS_DB"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("LETOPIS_REDIS_DB: %w", err)
		}
		cfg.Redis.DB = n
	}
	return nil
}
