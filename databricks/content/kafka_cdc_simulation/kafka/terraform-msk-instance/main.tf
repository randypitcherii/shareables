terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# Data sources for existing resources
data "aws_kms_key" "existing_kms_key" {
  key_id = "arn:aws:kms:us-east-1:332745928618:key/ed395791-9757-45be-bb9d-ea71eca17506"
}

# Initial MSK Configuration with allow.everyone.if.no.acl.found=true
resource "aws_msk_configuration" "msk_configuration_initial" {
  kafka_versions = ["3.8.x"]
  name           = "chill-out-kafka-initial"
  description    = "Initial configuration for randy-pitcher-workspace-mini MSK cluster with permissive ACL settings"

  server_properties = <<PROPERTIES
auto.create.topics.enable=true
default.replication.factor=1
num.partitions=10
replica.lag.time.max.ms=30000
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
unclean.leader.election.enable=false
delete.topic.enable=true
log.retention.ms=86400000
allow.everyone.if.no.acl.found=true
PROPERTIES

  lifecycle {
    create_before_destroy = true
  }
}

# Final MSK Configuration with allow.everyone.if.no.acl.found=false (after ACLs are set)
resource "aws_msk_configuration" "msk_configuration_final" {
  kafka_versions = ["3.8.x"]
  name           = "chill-out-kafka-final"
  description    = "Final configuration for randy-pitcher-workspace-mini MSK cluster with restrictive ACL settings"

  server_properties = <<PROPERTIES
auto.create.topics.enable=true
default.replication.factor=1
num.partitions=10
replica.lag.time.max.ms=30000
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
unclean.leader.election.enable=false
delete.topic.enable=true
log.retention.ms=86400000
allow.everyone.if.no.acl.found=false
PROPERTIES

  lifecycle {
    create_before_destroy = true
  }
}

# MSK Cluster (imported from existing)
resource "aws_msk_cluster" "randy_pitcher_workspace_mini" {
  cluster_name           = "randy-pitcher-workspace-mini"
  kafka_version          = "3.8.x"
  number_of_broker_nodes = 2
  enhanced_monitoring    = "DEFAULT"

  broker_node_group_info {
    az_distribution = "DEFAULT"
    client_subnets  = local.public_subnet_ids
    instance_type   = "kafka.t3.small"
    security_groups = [aws_security_group.msk_sg.id]

    connectivity_info {
      public_access {
        type = var.enable_public_access ? "SERVICE_PROVIDED_EIPS" : "DISABLED"
      }
    }

    storage_info {
      ebs_storage_info {
        volume_size = 1000
      }
    }
  }

  client_authentication {
    sasl {
      scram = true
      iam   = true
    }
    unauthenticated = false
  }

  configuration_info {
    arn      = var.enable_public_access ? aws_msk_configuration.msk_configuration_final.arn : aws_msk_configuration.msk_configuration_initial.arn
    revision = var.enable_public_access ? aws_msk_configuration.msk_configuration_final.latest_revision : aws_msk_configuration.msk_configuration_initial.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = data.aws_kms_key.existing_kms_key.arn

    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  tags = {
    owner = "randy.pitcher@databricks.com"
  }
}

# Add a new KMS key for SCRAM secret
resource "aws_kms_key" "msk_scram_secret_key" {
  description             = "KMS key for encrypting MSK SCRAM secret"
  deletion_window_in_days = 7
  policy                  = <<EOF
{
  "Version": "2012-10-17",
  "Id": "key-msk-scram-secret-policy",
  "Statement": [
    {
      "Sid": "Allow account root full access",
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"},
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Sid": "Allow MSK service to use key",
      "Effect": "Allow",
      "Principal": {"Service": "kafka.amazonaws.com"},
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}

# Add data source for current account id
data "aws_caller_identity" "current" {}

# Update the SCRAM secret to use the new KMS key
resource "aws_secretsmanager_secret" "msk_scram_secret" {
  name        = "AmazonMSK_randy_pitcher_workspace_mini_scram"
  description = "SCRAM secret for MSK cluster"
  kms_key_id  = aws_kms_key.msk_scram_secret_key.arn
}

resource "aws_secretsmanager_secret_version" "msk_scram_secret_version" {
  secret_id = aws_secretsmanager_secret.msk_scram_secret.id
  secret_string = jsonencode({
    username = var.kafka_username
    password = var.kafka_password
  })
}

# Associate SCRAM secret with MSK cluster
resource "aws_msk_scram_secret_association" "msk_scram_secret_association" {
  cluster_arn = aws_msk_cluster.randy_pitcher_workspace_mini.arn
  secret_arn_list = [
    aws_secretsmanager_secret.msk_scram_secret.arn
  ]
}

# Add data sources for default VPC and public subnets
data "aws_vpc" "default" {
  default = true
}

# Get availability zones
data "aws_availability_zones" "available" {
  state = "available"
}

# Hardcode subnets from different AZs
# These are public subnets in us-east-1a and us-east-1b
locals {
  public_subnet_ids = ["subnet-07480b8fbbb9501bd", "subnet-0a04badfd09d050b0"]
}

# Create new security group for MSK
resource "aws_security_group" "msk_sg" {
  name        = "msk-public-access-sg"
  description = "Security group for MSK public access"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 9092
    to_port     = 9198
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/1", "128.0.0.0/1"]
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/1", "128.0.0.0/1"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Add variable for public access toggle
variable "enable_public_access" {
  description = "Enable public access for MSK cluster"
  type        = bool
  default     = false
}
