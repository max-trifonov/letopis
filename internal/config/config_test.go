package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAppliesFileOverDefaults(t *testing.T) {
	path := writeFile(t, t.TempDir(), "config.yaml", `
role: api
server:
  http:
    addr: ":18080"
log:
  format: text
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Role != RoleAPI {
		t.Errorf("role = %q, want api", cfg.Role)
	}
	if got := cfg.Server.HTTP.Addr; got != ":18080" {
		t.Errorf("http addr = %q, want :18080", got)
	}
	// Untouched fields keep their defaults.
	if got := cfg.Server.GRPC.Addr; got != ":9090" {
		t.Errorf("grpc addr = %q, want default :9090", got)
	}
	if got := cfg.MongoDB.URI; got != "mongodb://localhost:27017" {
		t.Errorf("mongodb uri = %q, want default", got)
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	path := writeFile(t, t.TempDir(), "config.yaml", `
mongodb:
  uri: mongodb://from-file:27017
`)
	t.Setenv("LETOPIS_MONGODB_URI", "mongodb://from-env:27017")
	t.Setenv("LETOPIS_ROLE", "worker")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.MongoDB.URI; got != "mongodb://from-env:27017" {
		t.Errorf("mongodb uri = %q, want env value", got)
	}
	if cfg.Role != RoleWorker {
		t.Errorf("role = %q, want worker", cfg.Role)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeFile(t, t.TempDir(), "config.yaml", "rolle: api\n")
	if _, err := Load(path); err == nil {
		t.Fatal("want error on unknown field, got nil")
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	for name, content := range map[string]string{
		"bad role":           "role: superuser\n",
		"bad log level":      "log: {level: loud}\n",
		"tls without files":  "server: {http: {tls: {enabled: true}}}\n",
		"empty addr for api": "server: {http: {addr: \"\"}}\n",
	} {
		path := writeFile(t, t.TempDir(), "config.yaml", content)
		if _, err := Load(path); err == nil {
			t.Errorf("%s: want error, got nil", name)
		}
	}
}

// TestRoleResponsibilities pins the role split (ADR-006): api serves, worker
// consumes, all does both. The app wiring gates on exactly these two methods.
func TestRoleResponsibilities(t *testing.T) {
	for _, tc := range []struct {
		role             Role
		serves, consumes bool
	}{
		{RoleAPI, true, false},
		{RoleWorker, false, true},
		{RoleAll, true, true},
	} {
		if got := tc.role.ServesAPI(); got != tc.serves {
			t.Errorf("%s.ServesAPI() = %v, want %v", tc.role, got, tc.serves)
		}
		if got := tc.role.RunsWorker(); got != tc.consumes {
			t.Errorf("%s.RunsWorker() = %v, want %v", tc.role, got, tc.consumes)
		}
	}
}

func TestQueueValidation(t *testing.T) {
	for name, content := range map[string]string{
		"memory with split role": "role: worker\nqueue: {driver: memory}\n",
		"unknown driver":         "queue: {driver: kafka}\n",
		"zero shards":            "queue: {shards: 0}\n",
		"redis-streams no addr":  "redis: {addr: \"\"}\nqueue: {driver: redis-streams}\n",
	} {
		path := writeFile(t, t.TempDir(), "config.yaml", content)
		if _, err := Load(path); err == nil {
			t.Errorf("%s: want error, got nil", name)
		}
	}
}

func TestQueueMemoryValidForAll(t *testing.T) {
	path := writeFile(t, t.TempDir(), "config.yaml", "role: all\nqueue: {driver: memory}\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("memory + role=all should be valid: %v", err)
	}
	if cfg.Queue.Driver != "memory" {
		t.Errorf("driver = %q, want memory", cfg.Queue.Driver)
	}
	// Shards keeps its default when only the driver is overridden.
	if cfg.Queue.Shards != 16 {
		t.Errorf("shards = %d, want default 16", cfg.Queue.Shards)
	}
}

func TestResolveExplicitMustExist(t *testing.T) {
	if _, err := Resolve(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("want error for missing explicit path, got nil")
	}
}

func TestResolveFindsFileInWorkingDir(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, FileName, "role: all\n")
	t.Chdir(dir)

	got, err := Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, FileName) {
		t.Errorf("resolved %q, want path to %s", got, FileName)
	}
}
