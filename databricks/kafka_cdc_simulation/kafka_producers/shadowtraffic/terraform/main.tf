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

# Data source for Amazon Linux 2 AMI
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

# Data source for default VPC
data "aws_vpc" "default" {
  default = true
}

# Security group for ShadowTraffic EC2 instance
resource "aws_security_group" "shadowtraffic_sg" {
  name        = "shadowtraffic_sg"
  description = "Allow SSH from all"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/1", "128.0.0.0/1"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "shadowtraffic-sg"
  }
}

# EC2 instance for ShadowTraffic
resource "aws_instance" "shadowtraffic_producer" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = "m5.2xlarge"
  subnet_id     = var.subnets[0]  # Use first public subnet
  key_name      = "msk-bastion-key"  # Reused from MSK bastion

  vpc_security_group_ids = [aws_security_group.shadowtraffic_sg.id]

  # Expand root volume to avoid low disk space
  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e
    yum update -y || true
    amazon-linux-extras install docker -y || yum install -y docker || true
    systemctl start docker || service docker start || true
    systemctl enable docker || true
    usermod -a -G docker ec2-user || true
  EOF

  tags = {
    Name = "shadowtraffic-cdc-producer"
  }

  # Copy non-sensitive config and license.env
  provisioner "file" {
    source      = "${path.module}/../cdc_generator.json"
    destination = "/home/ec2-user/cdc_generator.json"
  }

  provisioner "file" {
    source      = "${path.module}/../license.env"
    destination = "/home/ec2-user/license.env"
  }

  # Run docker with env-file, injected secrets, and auto-restart
  provisioner "remote-exec" {
    inline = [
      # Ensure Docker is installed and running (in case user_data hasn't completed yet)
      "sudo yum update -y || true",
      "sudo amazon-linux-extras install docker -y || sudo yum install -y docker || true",
      "sudo systemctl start docker || sudo service docker start || true",
      "sudo systemctl enable docker || true",
      # Ensure filesystem is grown if volume was resized
      "FS_TYPE=$(df -T / | tail -1 | awk '{print $2}') && if [ \"$FS_TYPE\" = \"xfs\" ]; then sudo growpart /dev/nvme0n1 1 || true; sudo xfs_growfs -d / || true; else sudo resize2fs /dev/nvme0n1p1 || true; fi",
      # Pull images to avoid first-run delays
      "sudo docker pull shadowtraffic/shadowtraffic:latest || true",
      "sudo docker pull bitnami/kafka:3.6.1 || true",
      # Create Kafka client properties for SCRAM over TLS
      "cat > /home/ec2-user/kafka-client.properties <<PROPS\nsecurity.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${var.username}\" password=\"${var.password}\";\nPROPS",
      # Drop and recreate topic to start fresh each deployment
      "sudo docker run --rm -v /home/ec2-user/kafka-client.properties:/tmp/client.properties bitnami/kafka:3.6.1 \\",
      "  kafka-topics.sh --bootstrap-server ${var.kafka_brokers} --command-config /tmp/client.properties --delete --topic rpw_cdc_simulation__sad_lightning || true",
      "sudo docker run --rm -v /home/ec2-user/kafka-client.properties:/tmp/client.properties bitnami/kafka:3.6.1 \\",
      "  kafka-topics.sh --bootstrap-server ${var.kafka_brokers} --command-config /tmp/client.properties --create --topic rpw_cdc_simulation__sad_lightning --partitions 10 --replication-factor 2 || true",
      # Remove existing container if present to avoid name conflicts
      "sudo docker rm -f shadowtraffic || true",
      # Run the container
      "sudo docker run -d --name shadowtraffic --restart=unless-stopped \\",
      "  --env-file /home/ec2-user/license.env \\",
      "  -e KAFKA_TOPIC='rpw_cdc_simulation__sad_lightning' \\",
      "  -e KAFKA_BROKERS='${var.kafka_brokers}' \\",
      "  -e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${var.username}\" password=\"${var.password}\";' \\",
      "  -e RUN_STARTED_AT=\"$(date '+%Y-%m-%d %H:%M:%S')\" \\",
      "  -e PAYLOAD_STRING=\"$(printf '🪐%.0s' {1..7680})\" \\",
      "  -v /home/ec2-user/cdc_generator.json:/home/config.json \\",
      "  shadowtraffic/shadowtraffic:latest \\",
      "  --config /home/config.json"
    ]
  }

  connection {
    type        = "ssh"
    user        = "ec2-user"
    private_key = file("~/.ssh/msk-bastion-key.pem")
    host        = self.public_ip
  }
} 