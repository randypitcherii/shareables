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
  description = "EC2 instance type for Pulsar standalone. Sized up from the prior experiment's t3.large so the broker is not the bottleneck at 1KB-event scale."
  type        = string
  default     = "m6i.2xlarge"
}

variable "pulsar_version" {
  description = "Apache Pulsar image tag. Must match a released KoP version (KoP is archived; 3.1.1 / 3.1.1.1 is the final working pair)."
  type        = string
  default     = "3.1.1"
}

variable "kop_version" {
  description = "Kafka-on-Pulsar (KoP) protocol handler release tag, without the leading v. Must pair with pulsar_version."
  type        = string
  default     = "3.1.1.1"
}

variable "topic_partitions" {
  description = "Default partition count for auto-created topics (the eval topic)."
  type        = number
  default     = 4
}

variable "allowed_ingress_cidrs" {
  description = <<-EOT
    CIDRs allowed to reach Pulsar (6650), admin (8080), and Kafka/KoP (9092).
    No default on purpose: decide your own exposure. The broker runs
    UNAUTHENTICATED — synthetic data only; destroy when done. Note some AWS
    sandboxes reject a literal 0.0.0.0/0; use the two /1 halves
    ("0.0.0.0/1", "128.0.0.0/1") if you need world-open for a short run.
  EOT
  type        = list(string)
}

variable "ssh_public_key" {
  description = "SSH public key material. Required: the scale producer runs on the VM over SSH."
  type        = string
}

variable "ssh_ingress_cidr" {
  description = "CIDR allowed to SSH. Use your own /32 — never widen this."
  type        = string
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project   = "pulsar-uc-scale-and-freshness"
    ephemeral = "true"
  }
}
