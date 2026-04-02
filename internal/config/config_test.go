package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadForwardProbabilityInvalid(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("FORWARD_PROBABILITY", "1.5")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for probability > 1")
	}
}

func TestLoadForwardProbabilityNegative(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("FORWARD_PROBABILITY", "-0.1")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for probability < 0")
	}
}

func TestLoadDefaultForwardProbability(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("FORWARD_PROBABILITY", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ForwardProbability != 0.55 {
		t.Fatalf("default forward probability: got %v", cfg.ForwardProbability)
	}
	if cfg.WorkerConcurrency != 10 {
		t.Fatalf("default worker_concurrency: got %d", cfg.WorkerConcurrency)
	}
}

func TestLoadConfigYAMLInWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("log_level: debug\n"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_PATH", "") // Load must discover ./config.yaml in cwd
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("log_level: %q", cfg.LogLevel)
	}
}

func TestEnvOverridesFile(t *testing.T) {
	t.Chdir(t.TempDir())
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := `
log_level: debug
forward_probability: 0.5
kafka_brokers:
  - a:9092
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("FORWARD_PROBABILITY", "0.6")
	t.Setenv("KAFKA_BROKERS", "b:9092,c:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("log_level: want info from env, got %q", cfg.LogLevel)
	}
	if cfg.ForwardProbability != 0.6 {
		t.Fatalf("forward_probability: want 0.6 from env, got %v", cfg.ForwardProbability)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "b:9092" {
		t.Fatalf("kafka_brokers: %+v", cfg.KafkaBrokers)
	}
}

func TestReconnectMaxLessThanInitial(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("RECONNECT_INITIAL_DELAY", "5s")
	t.Setenv("RECONNECT_MAX_DELAY", "1s")
	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error when reconnect_max < reconnect_initial")
	}
}
