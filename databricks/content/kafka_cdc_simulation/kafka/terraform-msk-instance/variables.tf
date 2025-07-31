# Variables for the MSK cluster configuration
# These are primarily for documentation and potential future flexibility

variable "cluster_name" {
  description = "Name of the MSK cluster"
  type        = string
  default     = "randy-pitcher-workspace-mini"
}

variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "3.8.x"
}

variable "instance_type" {
  description = "Instance type for MSK brokers"
  type        = string
  default     = "kafka.t3.small"
}

variable "number_of_broker_nodes" {
  description = "Number of broker nodes"
  type        = number
  default     = 2
}

variable "ebs_volume_size" {
  description = "EBS volume size in GB"
  type        = number
  default     = 1000
}

variable "subnet_ids" {
  description = "Subnet IDs for the MSK cluster"
  type        = list(string)
  default = [
    "subnet-07480b8fbbb9501bd",
    "subnet-0a04badfd09d050b0"
  ]
}

variable "security_group_id" {
  description = "Security group ID for the MSK cluster"
  type        = string
  default     = "sg-08086c40bd2702665"
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption"
  type        = string
  default     = "arn:aws:kms:us-east-1:332745928618:key/ed395791-9757-45be-bb9d-ea71eca17506"
}

variable "existing_secret_name" {
  description = "Name of the existing SCRAM secret"
  type        = string
  default     = "AmazonMSK_kafka-admin"
}

variable "existing_config_name" {
  description = "Name of the existing MSK configuration"
  type        = string
  default     = "chill-out-kafka"
}

variable "kafka_username" {
  type        = string
  description = "Kafka username for MSK SCRAM authentication"
  sensitive   = true
}

variable "kafka_password" {
  type        = string
  description = "Kafka password for MSK SCRAM authentication"
  sensitive   = true
}
