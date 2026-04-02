package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"stuurwiel/internal/application/publish"
	"stuurwiel/internal/config"
	"stuurwiel/internal/infrastructure/broker"
	"stuurwiel/internal/infrastructure/transport"
	"stuurwiel/internal/logging"
	"stuurwiel/internal/runtime/reconnect"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logging.Stderr("info").Error("config load failed", "err", err)
		os.Exit(1)
	}
	logger := logging.New(cfg.LogLevel)
	slog.SetDefault(logger)

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutdown signal")
		cancelRoot()
	}()

	var natsPub *broker.NATSPublisher
	err = reconnect.DialUntil(rootCtx, logger, cfg, reconnect.NewAttemptLimiter(cfg.MaxReconnectAttempts), "nats publisher", func() error {
		var e error
		natsPub, e = broker.NewNATSPublisher(cfg.NATSURL, cfg.NATSSubject)
		return e
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(0)
		}
		logger.Error("nats publisher", "err", err)
		os.Exit(1)
	}
	defer natsPub.Close()

	kafkaPub := broker.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer kafkaPub.Close()

	err = reconnect.DialUntil(rootCtx, logger, cfg, reconnect.NewAttemptLimiter(cfg.MaxReconnectAttempts), "kafka", func() error {
		return broker.PingKafkaBrokers(rootCtx, cfg.KafkaBrokers)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(0)
		}
		logger.Error("kafka unreachable", "err", err)
		os.Exit(1)
	}

	var rabbitPub *broker.RabbitPublisher
	err = reconnect.DialUntil(rootCtx, logger, cfg, reconnect.NewAttemptLimiter(cfg.MaxReconnectAttempts), "rabbit publisher", func() error {
		var e error
		rabbitPub, e = broker.NewRabbitPublisher(cfg.RabbitURL, cfg.RabbitQueue)
		return e
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(0)
		}
		logger.Error("rabbit publisher", "err", err)
		os.Exit(1)
	}
	defer rabbitPub.Close()

	router := publish.NewPublishRouter(natsPub, kafkaPub, rabbitPub)
	svc := publish.NewService(router)

	mux := http.NewServeMux()
	transport.RegisterHTTP(mux, logger, svc)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	grpcSrv := grpc.NewServer()
	transport.RegisterGRPC(grpcSrv, logger, svc)

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("grpc listen", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("grpc listening", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Error("grpc serve", "err", err)
		}
	}()
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http serve", "err", err)
		}
	}()

	logger.Info("api ready", "reconnect_initial", cfg.ReconnectInitialDelay, "reconnect_max", cfg.ReconnectMaxDelay,
		"max_reconnect_attempts", cfg.MaxReconnectAttempts)

	<-rootCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	_ = grpcLis.Close()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "err", err)
	}
}
