terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "pulsar" {
  name_prefix = "pulsar-uc-eval-"
  description = "Apache Pulsar standalone for UC ingestion evaluation (ephemeral)"
  vpc_id      = data.aws_vpc.default.id

  # Pulsar binary protocol (native + Databricks pulsar connector)
  ingress {
    description = "Pulsar binary protocol"
    from_port   = 6650
    to_port     = 6650
    protocol    = "tcp"
    cidr_blocks = var.allowed_ingress_cidrs
  }

  # Pulsar admin/HTTP API (health checks, topic stats)
  ingress {
    description = "Pulsar admin API"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = var.allowed_ingress_cidrs
  }

  # Kafka protocol via KoP protocol handler
  ingress {
    description = "Kafka protocol (KoP)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = var.allowed_ingress_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.tags
}

# Standalone mode applies advertisedAddress to the embedded bookie too, so the
# bookie registers at <public-ip>:<ephemeral-port> and the broker's write path
# hairpins through the IGW back to its own public IP. Without this rule the SG
# drops those writes: ledgers open but no entry is ever confirmed
# (pendingAddEntriesCount grows, entriesAddedCounter stays 0) and producers,
# plus the /brokers/health probe, hang. The bookie port is ephemeral, so allow
# all TCP — but only from the VM's own public IP.
resource "aws_vpc_security_group_ingress_rule" "bookie_hairpin" {
  security_group_id = aws_security_group.pulsar.id
  description       = "Self-hairpin to embedded bookie (ephemeral port) via IGW"
  ip_protocol       = "tcp"
  from_port         = 1
  to_port           = 65535
  cidr_ipv4         = "${aws_instance.pulsar.public_ip}/32"
}

resource "aws_instance" "pulsar" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  vpc_security_group_ids      = [aws_security_group.pulsar.id]
  associate_public_ip_address = true

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    pulsar_version = var.pulsar_version
    kop_version    = var.kop_version
  })

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  tags = merge(var.tags, { Name = "pulsar-uc-eval" })
}
