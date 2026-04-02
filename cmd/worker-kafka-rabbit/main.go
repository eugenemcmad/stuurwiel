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
		return worker.ServeReconnect(ctx, logger, cfg, "kafka-rabbit", limiter, func(ctx context.Context) error {
			sub := broker.NewKafkaSubscriber(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
			defer sub.Close()

			var pub *broker.RabbitPublisher
			err := reconnect.DialUntil(ctx, logger, cfg, limiter, "rabbit publisher", func() error {
				var e error
				pub, e = broker.NewRabbitPublisher(cfg.RabbitURL, cfg.RabbitQueue)
				return e
			})
			if err != nil {
				return err
			}
			defer pub.Close()

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			edge := &relayapp.EdgeRelay{
				Log:         logger,
				SourceLabel: string(domain.Kafka),
				SinkLabel:   string(domain.RabbitMQ),
				Source:      sub,
				Sink:        pub,
				Workers:     cfg.WorkerConcurrency,
				Policy: &relayapp.StochasticForwardPolicy{
					P:   cfg.ForwardProbability,
					RNG: rng,
				},
			}

			logger.Info("session started", "edge", "kafka-rabbit", "forward_probability", cfg.ForwardProbability,
				"worker_concurrency", cfg.WorkerConcurrency,
				"reconnect_initial", cfg.ReconnectInitialDelay, "reconnect_max", cfg.ReconnectMaxDelay)
			return edge.Run(ctx)
		})
	})
}
