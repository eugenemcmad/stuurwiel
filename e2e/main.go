// Integration test driver: publishes via REST/gRPC against a running stack.
// -manage-compose runs docker compose up before tests and down on exit (make e2e enables this).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	stuurwielv1 "stuurwiel/api/grpc/stuurwiel/v1"
	"stuurwiel/internal/domain"
)

type row struct {
	apiPath string // path segment: rabbitmq, nats, kafka
	label   string // RMQ, NATS, Kafka
}

var plan = []row{
	{"rabbitmq", "RMQ"},
	{"nats", "NATS"},
	{"kafka", "Kafka"},
}

const restBatch = 5
const grpcBatch = 5

func totalPublishCount() int {
	return len(plan) * (restBatch + grpcBatch)
}

func main() {
	os.Exit(run())
}

func run() int {
	httpBase := flag.String("http", envOr("E2E_HTTP", "http://127.0.0.1:8080"), "REST base URL (no trailing slash)")
	grpcAddr := flag.String("grpc", envOr("E2E_GRPC", "127.0.0.1:9090"), "gRPC host:port")
	wait := flag.Duration("wait", 5*time.Minute, "wait until GET /healthz OK and gRPC TCP up (0 = skip)")
	observeAfter := flag.Duration("observe-after", envDuration("E2E_OBSERVE_AFTER", 45*time.Second), "after all publishes OK, stream worker logs or sleep this long (0 = skip)")
	composeDir := flag.String("compose-dir", envOr("E2E_COMPOSE_DIR", "."), "directory with compose.yaml (for docker compose logs)")
	streamWorkers := flag.Bool("stream-workers", envBool("E2E_STREAM_WORKERS", true), "during observe: docker compose logs -f worker-* (same terminal); off = sleep only")
	manageCompose := flag.Bool("manage-compose", false, "docker compose up -d before run; always docker compose down on exit (make e2e)")
	flag.Parse()

	composeDirClean := filepath.Clean(*composeDir)
	if *manageCompose {
		if err := dockerCompose(composeDirClean, "up", "-d"); err != nil {
			log.Printf("docker compose up: %v", err)
			if err2 := dockerCompose(composeDirClean, "down"); err2 != nil {
				log.Printf("docker compose down: %v", err2)
			}
			return 1
		}
		defer func() {
			if err := dockerCompose(composeDirClean, "down"); err != nil {
				log.Printf("docker compose down: %v", err)
			}
		}()
	}

	ctx := context.Background()
	if *wait > 0 {
		if err := waitReady(*httpBase, *grpcAddr, *wait); err != nil {
			log.Printf("wait: %v", err)
			return 1
		}
	}

	conn, err := grpc.NewClient(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("grpc dial: %v", err)
		return 1
	}
	defer conn.Close()
	gcli := stuurwielv1.NewMessageServiceClient(conn)

	httpClient := &http.Client{Timeout: 60 * time.Second}

	var failed int
	var nextID int64
	for range restBatch {
		for _, p := range plan {
			nextID++
			msg := domain.Msg{EventID: nextID, Text: fmt.Sprintf("REST -> %s", p.label)}
			if err := postREST(ctx, httpClient, *httpBase, p.apiPath, msg); err != nil {
				log.Printf("REST %s msg=%+v: %v", p.apiPath, msg, err)
				failed++
			} else {
				log.Printf("ok REST %s msg=%+v", p.apiPath, msg)
			}
		}
	}
	for range grpcBatch {
		for _, p := range plan {
			nextID++
			text := fmt.Sprintf("gRPC -> %s", p.label)
			gmsg := domain.Msg{EventID: nextID, Text: text}
			_, err := gcli.Publish(ctx, &stuurwielv1.PublishRequest{
				Broker:  p.apiPath,
				EventId: nextID,
				Text:    text,
			})
			if err != nil {
				log.Printf("gRPC %s msg=%+v: %v", p.apiPath, gmsg, err)
				failed++
			} else {
				log.Printf("ok gRPC %s msg=%+v", p.apiPath, gmsg)
			}
		}
	}

	if failed > 0 {
		log.Printf("e2e finished with %d error(s)", failed)
		return 1
	}
	log.Printf("e2e: all %d publishes succeeded", totalPublishCount())
	if *observeAfter > 0 {
		observeRelay(*observeAfter, composeDirClean, *streamWorkers)
	}
	return 0
}

func dockerCompose(dir string, args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}

// observeRelay streams worker container logs or sleeps for the observe window.
func observeRelay(d time.Duration, composeDir string, streamWorkers bool) {
	composeDir = filepath.Clean(composeDir)
	if !streamWorkers {
		log.Printf("e2e: waiting %s (--stream-workers=false; relay logs only in docker)", d)
		time.Sleep(d)
		log.Printf("e2e: observe window finished")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "logs", "-f", "--since", "10m",
		"worker-nats-kafka", "worker-kafka-rabbit", "worker-rabbit-nats")
	cmd.Dir = composeDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("e2e: streaming worker container logs for %s (action=forward / message relayed between brokers)", d)
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		log.Printf("e2e: observe window finished")
		return
	}
	if err != nil {
		log.Printf("e2e: docker compose logs failed (%v) — run from repo root or -compose-dir; sleeping %s", err, d)
		time.Sleep(d)
	}
	log.Printf("e2e: observe window finished")
}

func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes"
}

func hostPortFromHTTPBase(base string) (string, error) {
	u := strings.TrimPrefix(strings.TrimPrefix(base, "https://"), "http://")
	if i := strings.IndexByte(u, '/'); i >= 0 {
		u = u[:i]
	}
	if u == "" {
		return "", fmt.Errorf("empty host in http base %q", base)
	}
	return u, nil
}

// waitReady waits for GET /healthz and gRPC TCP (avoids POST EOF before HTTP is ready).
func waitReady(httpBase, grpcAddr string, total time.Duration) error {
	httpHost, err := hostPortFromHTTPBase(httpBase)
	if err != nil {
		return err
	}
	healthURL := strings.TrimRight(httpBase, "/") + "/healthz"
	deadline := time.Now().Add(total)
	var lastErr error
	client := &http.Client{Timeout: 3 * time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, healthURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				e2 := dialOnce(grpcAddr)
				if e2 == nil {
					log.Printf("wait: GET /healthz OK and %s / %s ready", httpHost, grpcAddr)
					return nil
				}
				lastErr = e2
			} else {
				lastErr = fmt.Errorf("healthz HTTP %s", resp.Status)
			}
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout after %v (last: %v)", total, lastErr)
}

func dialOnce(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	return c.Close()
}

func postREST(ctx context.Context, client *http.Client, base, broker string, m domain.Msg) error {
	const attempts = 4
	var lastErr error
	for i := 0; i < attempts; i++ {
		lastErr = postRESTOnce(ctx, client, base, broker, m)
		if lastErr == nil {
			return nil
		}
		if i < attempts-1 && isRetriableHTTP(lastErr) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		break
	}
	return lastErr
}

func isRetriableHTTP(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne *net.OpError
	if errors.As(err, &ne) {
		return ne.Err != nil && (errors.Is(ne.Err, io.EOF) || errors.Is(ne.Err, io.ErrUnexpectedEOF))
	}
	return false
}

func postRESTOnce(ctx context.Context, client *http.Client, base, broker string, m domain.Msg) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	url := base + "/v1/publish/" + broker
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	return nil
}
