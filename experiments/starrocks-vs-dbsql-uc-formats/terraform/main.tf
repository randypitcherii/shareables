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
# never reconciles the rule set — inline blocks treat externally-added rules
# as drift and silently delete them on the next apply.
resource "aws_security_group" "starrocks" {
  name_prefix = "starrocks-uc-eval-"
  description = "StarRocks allin1 for UC format evaluation (ephemeral)"
  vpc_id      = data.aws_vpc.default.id

  tags = var.tags
}

locals {
  starrocks_ports = {
    "FE MySQL protocol (query port)" = 9030
    "FE HTTP (stream load, UI)"      = 8030
    "BE HTTP (stream load redirect)" = 8040
  }
  # one rule resource per (port, cidr) pair
  starrocks_ingress = {
    for pair in setproduct(keys(local.starrocks_ports), var.allowed_ingress_cidrs) :
    "${local.starrocks_ports[pair[0]]}-${pair[1]}" => {
      description = pair[0]
      port        = local.starrocks_ports[pair[0]]
      cidr        = pair[1]
    }
  }
}

resource "aws_vpc_security_group_ingress_rule" "service" {
  for_each          = local.starrocks_ingress
  security_group_id = aws_security_group.starrocks.id
  description       = each.value.description
  ip_protocol       = "tcp"
  from_port         = each.value.port
  to_port           = each.value.port
  cidr_ipv4         = each.value.cidr
}

locals {
  ssh_enabled = var.ssh_public_key != "" && var.ssh_ingress_cidr != ""
}

resource "aws_key_pair" "starrocks" {
  count      = local.ssh_enabled ? 1 : 0
  key_name   = "starrocks-uc-eval"
  public_key = var.ssh_public_key
  tags       = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  count             = local.ssh_enabled ? 1 : 0
  security_group_id = aws_security_group.starrocks.id
  description       = "SSH for container log access"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = var.ssh_ingress_cidr
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.starrocks.id
  description       = "All egress (docker pull, UC Iceberg REST, vended S3 credentials)"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_instance" "starrocks" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  vpc_security_group_ids      = [aws_security_group.starrocks.id]
  associate_public_ip_address = true
  key_name                    = local.ssh_enabled ? aws_key_pair.starrocks[0].key_name : null

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    starrocks_version = var.starrocks_version
  })

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  tags = merge(var.tags, { Name = "starrocks-uc-eval" })
}
