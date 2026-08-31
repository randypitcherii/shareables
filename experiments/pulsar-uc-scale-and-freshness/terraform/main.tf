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

# Rules live in standalone resources (not inline blocks) so the SG resource
# never reconciles the rule set — inline blocks treat any rule added by another
# resource as drift and silently delete it on the next apply.
resource "aws_security_group" "pulsar" {
  name_prefix = "pulsar-uc-scale-"
  description = "Apache Pulsar standalone for UC scale/freshness evaluation (ephemeral)"
  vpc_id      = data.aws_vpc.default.id

  tags = var.tags
}

locals {
  pulsar_ports = {
    "Pulsar binary protocol" = 6650
    "Pulsar admin API"       = 8080
    "Kafka protocol (KoP)"   = 9092
  }
  pulsar_ingress = {
    for pair in setproduct(keys(local.pulsar_ports), var.allowed_ingress_cidrs) :
    "${local.pulsar_ports[pair[0]]}-${pair[1]}" => {
      description = pair[0]
      port        = local.pulsar_ports[pair[0]]
      cidr        = pair[1]
    }
  }
}

resource "aws_vpc_security_group_ingress_rule" "service" {
  for_each          = local.pulsar_ingress
  security_group_id = aws_security_group.pulsar.id
  description       = each.value.description
  ip_protocol       = "tcp"
  from_port         = each.value.port
  to_port           = each.value.port
  cidr_ipv4         = each.value.cidr
}

# SSH is REQUIRED in this experiment: the scale/freshness producer runs ON the
# broker VM (laptop WAN bandwidth would otherwise be the bottleneck), and the
# remote-produce make targets drive it over SSH.
resource "aws_key_pair" "pulsar" {
  key_name   = "pulsar-uc-scale-eval"
  public_key = var.ssh_public_key
  tags       = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.pulsar.id
  description       = "SSH for the on-VM producer and broker log access"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = var.ssh_ingress_cidr
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.pulsar.id
  description       = "All egress"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_instance" "pulsar" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  vpc_security_group_ids      = [aws_security_group.pulsar.id]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.pulsar.key_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    pulsar_version   = var.pulsar_version
    kop_version      = var.kop_version
    topic_partitions = var.topic_partitions
  })

  # NOTE: sized to fit sandbox SCP limits — larger volumes / provisioned
  # iops+throughput hit an explicit deny (ec2:RunInstances on volume/*).
  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  tags = merge(var.tags, { Name = "pulsar-uc-scale-eval" })
}
