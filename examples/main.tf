terraform {
  required_providers {
    kafka = {
      source = "mongey/kafka"
    }
  }
}

provider "kafka" {
  region = "us-east-1"
}

resource "kafka_topic" "syslog" {
  name               = "syslog"
  replication_factor = 1
  partitions         = 4

  config = {
    "segment.ms"   = "4000"
    "retention.ms" = "86400000"
  }
  bootstrap_server = "localhost:9098"
}
