# Stuurwiel stack for Nomad (same services as docker-compose / k8s).
# All broker and app tasks run in one task group so they share a network namespace;
# connection URLs use 127.0.0.1 (see ../README.md).

variable "app_image" {
  type        = string
  description = "Image for api and worker-* tasks (from Dockerfile)."
  default     = "stuurwiel:local"
}

job "stuurwiel" {
  datacenters = ["*"]
  type        = "service"

  group "stack" {
    count = 1

    network {
      mode = "bridge"
      port "http" {
        static = 8080
      }
      port "grpc" {
        static = 9090
      }
    }

    task "kafka" {
      driver = "docker"

      config {
        image         = "confluentinc/cp-kafka:7.5.0"
        hostname      = "kafka"
        image_pull_policy = "missing"
      }

      env {
        KAFKA_NODE_ID                                = "1"
        KAFKA_PROCESS_ROLES                          = "broker,controller"
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP         = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
        KAFKA_LISTENERS                              = "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092"
        KAFKA_ADVERTISED_LISTENERS                   = "PLAINTEXT://127.0.0.1:29092,PLAINTEXT_HOST://127.0.0.1:9092"
        KAFKA_INTER_BROKER_LISTENER_NAME             = "PLAINTEXT"
        KAFKA_CONTROLLER_LISTENER_NAMES              = "CONTROLLER"
        KAFKA_CONTROLLER_QUORUM_VOTERS               = "1@127.0.0.1:29093"
        KAFKA_LOG_DIRS                               = "/tmp/kraft-combined-logs"
        CLUSTER_ID                                   = "MkU3OEVBNTcwNTJENDM2Qk"
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR       = "1"
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR          = "1"
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "1"
        KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS       = "0"
        KAFKA_AUTO_CREATE_TOPICS_ENABLE              = "true"
      }

      resources {
        cpu    = 500
        memory = 1024
      }
    }

    task "nats" {
      driver = "docker"

      config {
        image             = "nats:2.10-alpine"
        image_pull_policy = "missing"
      }

      resources {
        cpu    = 100
        memory = 256
      }
    }

    task "rabbitmq" {
      driver = "docker"

      config {
        image             = "rabbitmq:3.13-management-alpine"
        image_pull_policy = "missing"
      }

      env {
        RABBITMQ_SERVER_ADDITIONAL_ERLANG_ARGS = "-rabbit loopback_users []"
      }

      resources {
        cpu    = 200
        memory = 512
      }
    }

    task "api" {
      driver = "docker"

      config {
        image             = var.app_image
        image_pull_policy = "missing"
        command           = "/usr/local/bin/api"
        ports             = ["http", "grpc"]
      }

      env {
        LOG_LEVEL              = "debug"
        NATS_URL               = "nats://127.0.0.1:4222"
        KAFKA_BROKERS          = "127.0.0.1:29092"
        RABBIT_URL             = "amqp://guest:guest@127.0.0.1:5672/"
        HTTP_ADDR              = ":8080"
        GRPC_ADDR              = ":9090"
        MAX_RECONNECT_ATTEMPTS = "100"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }

    task "worker-nats-kafka" {
      driver = "docker"

      config {
        image             = var.app_image
        image_pull_policy = "missing"
        command           = "/usr/local/bin/worker-nats-kafka"
      }

      env {
        LOG_LEVEL               = "info"
        FORWARD_PROBABILITY     = "0.55"
        WORKER_CONCURRENCY      = "10"
        NATS_URL                = "nats://127.0.0.1:4222"
        NATS_QUEUE_GROUP        = "stuurwiel-worker-nats-kafka"
        KAFKA_BROKERS           = "127.0.0.1:29092"
        RABBIT_URL              = "amqp://guest:guest@127.0.0.1:5672/"
        RECONNECT_INITIAL_DELAY = "2s"
        RECONNECT_MAX_DELAY     = "2m"
        RECONNECT_JITTER_MAX    = "50ms"
        MAX_RECONNECT_ATTEMPTS  = "100"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }

    task "worker-kafka-rabbit" {
      driver = "docker"

      config {
        image             = var.app_image
        image_pull_policy = "missing"
        command           = "/usr/local/bin/worker-kafka-rabbit"
      }

      env {
        LOG_LEVEL               = "info"
        FORWARD_PROBABILITY     = "0.55"
        WORKER_CONCURRENCY      = "10"
        NATS_URL                = "nats://127.0.0.1:4222"
        KAFKA_BROKERS           = "127.0.0.1:29092"
        KAFKA_GROUP_ID          = "stuurwiel-worker-kafka-rabbit"
        RABBIT_URL              = "amqp://guest:guest@127.0.0.1:5672/"
        RECONNECT_INITIAL_DELAY = "2s"
        RECONNECT_MAX_DELAY     = "2m"
        RECONNECT_JITTER_MAX    = "50ms"
        MAX_RECONNECT_ATTEMPTS  = "100"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }

    task "worker-rabbit-nats" {
      driver = "docker"

      config {
        image             = var.app_image
        image_pull_policy = "missing"
        command           = "/usr/local/bin/worker-rabbit-nats"
      }

      env {
        LOG_LEVEL               = "info"
        FORWARD_PROBABILITY     = "0.55"
        WORKER_CONCURRENCY      = "10"
        NATS_URL                = "nats://127.0.0.1:4222"
        KAFKA_BROKERS           = "127.0.0.1:29092"
        RABBIT_URL              = "amqp://guest:guest@127.0.0.1:5672/"
        RECONNECT_INITIAL_DELAY = "2s"
        RECONNECT_MAX_DELAY     = "2m"
        RECONNECT_JITTER_MAX    = "50ms"
        MAX_RECONNECT_ATTEMPTS  = "100"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }
  }
}
