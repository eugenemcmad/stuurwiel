// Package infrastructure is not a single Go package; see subpackages:
//
//   - broker: NATS, Kafka, RabbitMQ adapters
//   - transport: HTTP and gRPC adapters into the application layer
//
// Adapters depend on domain and application ports; nothing in domain should import here.
package infrastructure
