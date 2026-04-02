package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds shared settings: defaults, then optional YAML in cwd or CONFIG_PATH, then environment (env wins).
type Config struct {
	LogLevel string

	NATSURL        string
	NATSSubject    string
	NATSQueueGroup string

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	RabbitURL   string
	RabbitQueue string

	ForwardProbability float64
	// WorkerConcurrency is how many relay handler goroutines run per edge (each consumer process).
	WorkerConcurrency int
	HTTPAddr          string
	GRPCAddr          string

	// ReconnectInitialDelay is the first wait when (re)dialing brokers or after a session error.
	// ReconnectMaxDelay caps exponential backoff (delay doubles each failure until cap).
	ReconnectInitialDelay time.Duration
	ReconnectMaxDelay     time.Duration
	// ReconnectJitterMax adds uniform random delay in [0, ReconnectJitterMax] on top of each wait (0 disables).
	ReconnectJitterMax time.Duration
	// MaxReconnectAttempts is the total number of failed dial or session attempts before the worker exits with an error.
	MaxReconnectAttempts int
}

// defaultConfig is the baseline when no file and no env overrides apply.
func defaultConfig() Config {
	return Config{
		LogLevel: "info",

		NATSURL:        "nats://127.0.0.1:4222",
		NATSSubject:    "stuurwiel.messages",
		NATSQueueGroup: "stuurwiel-relay",

		KafkaBrokers: []string{"127.0.0.1:9092"},
		KafkaTopic:   "stuurwiel-messages",
		KafkaGroupID: "stuurwiel-relay",

		RabbitURL:   "amqp://guest:guest@127.0.0.1:5672/",
		RabbitQueue: "stuurwiel.messages",

		ForwardProbability: 0.55,
		WorkerConcurrency:  10,
		HTTPAddr:           ":8080",
		GRPCAddr:           ":9090",

		ReconnectInitialDelay: 2 * time.Second,
		ReconnectMaxDelay:     1 * time.Minute,
		ReconnectJitterMax:    100 * time.Millisecond,
		MaxReconnectAttempts:  10,
	}
}

// Load builds Config: defaults, then optional file (see Config doc), then env (env wins when the variable is set).
func Load() (Config, error) {
	cfg := defaultConfig()

	if path, ok := resolveConfigFilePath(); ok {
		b, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file %q: %w", path, err)
		}
		if err := mergeFileIntoConfig(&cfg, path, b); err != nil {
			return Config{}, fmt.Errorf("parse config file %q: %w", path, err)
		}
	}

	if err := applyEnvOverrides(&cfg); err != nil {
		return Config{}, err
	}

	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func mergeFileIntoConfig(cfg *Config, path string, b []byte) error {
	ext := strings.ToLower(filepath.Ext(path))
	var m map[string]interface{}
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(b, &m); err != nil {
			return err
		}
	case ".json":
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported config extension %q (use .yaml, .yml, or .json)", ext)
	}
	return applyMapToConfig(cfg, m)
}

func applyMapToConfig(cfg *Config, m map[string]interface{}) error {
	if v, ok := m["log_level"].(string); ok {
		cfg.LogLevel = v
	}
	if v, ok := m["nats_url"].(string); ok {
		cfg.NATSURL = v
	}
	if v, ok := m["nats_subject"].(string); ok {
		cfg.NATSSubject = v
	}
	if v, ok := m["nats_queue_group"].(string); ok {
		cfg.NATSQueueGroup = v
	}
	if v, ok := m["kafka_topic"].(string); ok {
		cfg.KafkaTopic = v
	}
	if v, ok := m["kafka_group_id"].(string); ok {
		cfg.KafkaGroupID = v
	}
	if v, ok := m["rabbit_url"].(string); ok {
		cfg.RabbitURL = v
	}
	if v, ok := m["rabbit_queue"].(string); ok {
		cfg.RabbitQueue = v
	}
	if v, ok := m["http_addr"].(string); ok {
		cfg.HTTPAddr = v
	}
	if v, ok := m["grpc_addr"].(string); ok {
		cfg.GRPCAddr = v
	}

	if v, ok := m["forward_probability"]; ok {
		f, err := parseFloat(v)
		if err != nil {
			return fmt.Errorf("forward_probability: %w", err)
		}
		cfg.ForwardProbability = f
	}
	if v, ok := m["worker_concurrency"]; ok {
		n, err := parseIntAny(v)
		if err != nil {
			return fmt.Errorf("worker_concurrency: %w", err)
		}
		cfg.WorkerConcurrency = n
	}

	if v, ok := m["kafka_brokers"]; ok {
		brokers, err := parseKafkaBrokers(v)
		if err != nil {
			return fmt.Errorf("kafka_brokers: %w", err)
		}
		cfg.KafkaBrokers = brokers
	}

	if v, ok := m["reconnect_initial_delay"]; ok {
		d, err := parseDurationAny(v)
		if err != nil {
			return fmt.Errorf("reconnect_initial_delay: %w", err)
		}
		cfg.ReconnectInitialDelay = d
	}
	if v, ok := m["reconnect_max_delay"]; ok {
		d, err := parseDurationAny(v)
		if err != nil {
			return fmt.Errorf("reconnect_max_delay: %w", err)
		}
		cfg.ReconnectMaxDelay = d
	}
	if v, ok := m["reconnect_jitter_max"]; ok {
		d, err := parseDurationAny(v)
		if err != nil {
			return fmt.Errorf("reconnect_jitter_max: %w", err)
		}
		cfg.ReconnectJitterMax = d
	}
	if v, ok := m["max_reconnect_attempts"]; ok {
		n, err := parseIntAny(v)
		if err != nil {
			return fmt.Errorf("max_reconnect_attempts: %w", err)
		}
		cfg.MaxReconnectAttempts = n
	}

	return nil
}

func parseIntAny(v interface{}) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case string:
		return strconv.Atoi(x)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func parseDurationAny(v interface{}) (time.Duration, error) {
	switch x := v.(type) {
	case string:
		return time.ParseDuration(x)
	case int:
		return time.Duration(x) * time.Second, nil
	case int64:
		return time.Duration(x) * time.Second, nil
	case float64:
		return time.Duration(x * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func parseFloat(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(x, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func parseKafkaBrokers(v interface{}) ([]string, error) {
	switch x := v.(type) {
	case string:
		return splitBrokers(x), nil
	case []interface{}:
		out := make([]string, 0, len(x))
		for _, e := range x {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("expected string broker address, got %T", e)
			}
			if s != "" {
				out = append(out, s)
			}
		}
		return out, nil
	case []string:
		return append([]string(nil), x...), nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func applyEnvOverrides(cfg *Config) error {
	// Strings: set when the variable is present (even empty).
	if v, ok := os.LookupEnv("LOG_LEVEL"); ok {
		cfg.LogLevel = v
	}
	if v, ok := os.LookupEnv("NATS_URL"); ok {
		cfg.NATSURL = v
	}
	if v, ok := os.LookupEnv("NATS_SUBJECT"); ok {
		cfg.NATSSubject = v
	}
	if v, ok := os.LookupEnv("NATS_QUEUE_GROUP"); ok {
		cfg.NATSQueueGroup = v
	}
	if v, ok := os.LookupEnv("KAFKA_TOPIC"); ok {
		cfg.KafkaTopic = v
	}
	if v, ok := os.LookupEnv("KAFKA_GROUP_ID"); ok {
		cfg.KafkaGroupID = v
	}
	if v, ok := os.LookupEnv("RABBIT_URL"); ok {
		cfg.RabbitURL = v
	}
	if v, ok := os.LookupEnv("RABBIT_QUEUE"); ok {
		cfg.RabbitQueue = v
	}
	if v, ok := os.LookupEnv("HTTP_ADDR"); ok {
		cfg.HTTPAddr = v
	}
	if v, ok := os.LookupEnv("GRPC_ADDR"); ok {
		cfg.GRPCAddr = v
	}

	if v, ok := os.LookupEnv("KAFKA_BROKERS"); ok && v != "" {
		cfg.KafkaBrokers = splitBrokers(v)
	}

	if v, ok := os.LookupEnv("FORWARD_PROBABILITY"); ok && v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("FORWARD_PROBABILITY: %w", err)
		}
		cfg.ForwardProbability = f
	}
	if v, ok := os.LookupEnv("WORKER_CONCURRENCY"); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("WORKER_CONCURRENCY: %w", err)
		}
		cfg.WorkerConcurrency = n
	}

	if v, ok := os.LookupEnv("RECONNECT_INITIAL_DELAY"); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("RECONNECT_INITIAL_DELAY: %w", err)
		}
		cfg.ReconnectInitialDelay = d
	}
	if v, ok := os.LookupEnv("RECONNECT_MAX_DELAY"); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("RECONNECT_MAX_DELAY: %w", err)
		}
		cfg.ReconnectMaxDelay = d
	}
	if v, ok := os.LookupEnv("RECONNECT_JITTER_MAX"); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("RECONNECT_JITTER_MAX: %w", err)
		}
		cfg.ReconnectJitterMax = d
	}
	if v, ok := os.LookupEnv("MAX_RECONNECT_ATTEMPTS"); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MAX_RECONNECT_ATTEMPTS: %w", err)
		}
		cfg.MaxReconnectAttempts = n
	}
	return nil
}

func validate(cfg Config) error {
	if cfg.ForwardProbability < 0 || cfg.ForwardProbability > 1 {
		return fmt.Errorf("FORWARD_PROBABILITY must be between 0 and 1, got %v", cfg.ForwardProbability)
	}
	if cfg.ReconnectInitialDelay <= 0 {
		return fmt.Errorf("reconnect_initial_delay must be > 0, got %v", cfg.ReconnectInitialDelay)
	}
	if cfg.ReconnectMaxDelay < cfg.ReconnectInitialDelay {
		return fmt.Errorf("reconnect_max_delay (%v) must be >= reconnect_initial_delay (%v)", cfg.ReconnectMaxDelay, cfg.ReconnectInitialDelay)
	}
	if cfg.ReconnectJitterMax < 0 {
		return fmt.Errorf("reconnect_jitter_max must be >= 0, got %v", cfg.ReconnectJitterMax)
	}
	if cfg.MaxReconnectAttempts < 1 {
		return fmt.Errorf("max_reconnect_attempts must be >= 1, got %d", cfg.MaxReconnectAttempts)
	}
	if cfg.WorkerConcurrency < 1 {
		return fmt.Errorf("worker_concurrency must be >= 1, got %d", cfg.WorkerConcurrency)
	}
	return nil
}

func splitBrokers(s string) []string {
	if s == "" {
		return nil
	}
	out := make([]string, 0, 1)
	for start := 0; start < len(s); {
		end := start
		for end < len(s) && s[end] != ',' {
			end++
		}
		part := s[start:end]
		if part != "" {
			out = append(out, part)
		}
		start = end + 1
	}
	return out
}

// resolveConfigFilePath returns a path to load when CONFIG_PATH is set, or when ./config.yaml / ./config.yml exists.
// CONFIG_PATH takes precedence when non-empty.
func resolveConfigFilePath() (string, bool) {
	if p := strings.TrimSpace(os.Getenv("CONFIG_PATH")); p != "" {
		return p, true
	}
	for _, name := range []string{"config.yaml", "config.yml"} {
		if st, err := os.Stat(name); err == nil && !st.IsDir() {
			return name, true
		}
	}
	return "", false
}
