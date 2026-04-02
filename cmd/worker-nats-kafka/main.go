package main

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"time"

	relayapp "stuurwiel/internal/application/relay"
	"stuurwiel/internal/config"
	"stuurwiel/internal/domain"
	"stuurwiel/internal/infrastructure/broker"
	"stuurwiel/internal/logging"
	"stuurwiel/internal/runtime/lifecycle"
	"stuurwiel/internal/runtime/reconnect"
	"stuurwiel/internal/runtime/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logging.Stderr("info").Error("config load failed", "err", err)
		os.Exit(1)
	}
	logger := logging.New(cfg.LogLevel)
	slog.SetDefault(logger)

	limiter := reconnect.NewAttemptLimiter(cfg.MaxReconnectAttempts)

	lifecycle.RunContext(logger, func(ctx context.Context) error {
		return worker.ServeReconnect(ctx, logger, cfg, "nats-kafka", limiter, func(ctx context.Context) error {
			var sub *broker.NATSSubscriber
			err := reconnect.DialUntil(ctx, logger, cfg, limiter, "nats subscriber", func() error {
				var e error
				sub, e = broker.NewNATSSubscriber(cfg.NATSURL, cfg.NATSSubject, cfg.NATSQueueGroup)
				return e
			})
			if err != nil {
				return err
			}
			defer sub.Close()

			pub := broker.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
			defer pub.Close()

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			edge := &relayapp.EdgeRelay{
				Log:         logger,
				SourceLabel: string(domain.NATS),
				SinkLabel:   string(domain.Kafka),
				Source:      sub,
				Sink:        pub,
				Workers:     cfg.WorkerConcurrency,
				Policy: &relayapp.StochasticForwardPolicy{
					P:   cfg.ForwardProbability,
					RNG: rng,
				},
			}

			logger.Info("session started", "edge", "nats-kafka", "forward_probability", cfg.ForwardProbability,
				"worker_concurrency", cfg.WorkerConcurrency,
				"reconnect_initial", cfg.ReconnectInitialDelay, "reconnect_max", cfg.ReconnectMaxDelay)
			return edge.Run(ctx)
		})
	})
}
