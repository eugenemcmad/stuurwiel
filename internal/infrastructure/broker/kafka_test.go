package broker

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPingKafkaBrokers_Empty(t *testing.T) {
	err := PingKafkaBrokers(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	err = PingKafkaBrokers(context.Background(), []string{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPingKafkaBrokers_LocalListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_ = c.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := PingKafkaBrokers(ctx, []string{ln.Addr().String()}); err != nil {
		t.Fatal(err)
	}
}

func TestPingKafkaBrokers_NoListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := PingKafkaBrokers(ctx, []string{"127.0.0.1:1"})
	if err == nil {
		t.Fatal("expected error when nothing listens")
	}
}
