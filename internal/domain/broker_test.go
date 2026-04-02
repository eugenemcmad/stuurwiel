package domain

import "testing"

func TestHopPathSuffix(t *testing.T) {
	if HopPathSuffix(NATS) != " -> NATS" {
		t.Fatal(HopPathSuffix(NATS))
	}
	if HopPathSuffix(Kafka) != " -> Kafka" {
		t.Fatal(HopPathSuffix(Kafka))
	}
	if HopPathSuffix(RabbitMQ) != " -> RMQ" {
		t.Fatal(HopPathSuffix(RabbitMQ))
	}
}

func TestNextRingOrder(t *testing.T) {
	if NextBroker(NATS) != Kafka {
		t.Fatal()
	}
	if NextBroker(Kafka) != RabbitMQ {
		t.Fatal()
	}
	if NextBroker(RabbitMQ) != NATS {
		t.Fatal()
	}
}
