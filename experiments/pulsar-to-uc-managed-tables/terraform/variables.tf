variable "aws_region" {
  description = "AWS region for the Pulsar VM"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type for Pulsar standalone"
  type        = string
  default     = "t3.large"
}

variable "pulsar_version" {
  description = "Apache Pulsar image tag. Must match a released KoP version (KoP tracks Pulsar minor versions)."
  type        = string
  default     = "3.1.1"
}

variable "kop_version" {
  description = "Kafka-on-Pulsar (KoP) protocol handler release tag, without the leading v. Must pair with pulsar_version."
  type        = string
  default     = "3.1.1.1"
}

variable "allowed_ingress_cidrs" {
  description = <<-EOT
    CIDRs allowed to reach Pulsar (6650), admin (8080), and Kafka/KoP (9092).
    No default on purpose: decide your own exposure. For a short-lived evaluation
    with synthetic data you may choose ["0.0.0.0/0"]; lock to your laptop IP and
    your Databricks workspace egress IPs for anything longer-lived. The broker
    runs UNAUTHENTICATED — never point real data at this.
  EOT
  type        = list(string)
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project   = "pulsar-to-uc-managed-tables"
    ephemeral = "true"
  }
}
