# ==============================================================================
# EC2 BASTION HOST
# ==============================================================================

# Security group for bastion host
resource "aws_security_group" "bastion_sg" {
  name        = "bastion-sg"
  description = "Security group for bastion host"
  vpc_id      = data.aws_vpc.default.id

  # SSH access from anywhere (for testing)
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/1", "128.0.0.0/1"]
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name  = "bastion-sg"
    owner = "randy.pitcher@databricks.com"
  }
}

# IAM role for bastion
resource "aws_iam_role" "bastion_role" {
  name = "msk-bastion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = {
    owner = "randy.pitcher@databricks.com"
  }
}

resource "aws_iam_role_policy_attachment" "bastion_msk_access" {
  role       = aws_iam_role.bastion_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonMSKFullAccess"
}

resource "aws_iam_instance_profile" "bastion_profile" {
  name = "msk-bastion-profile"
  role = aws_iam_role.bastion_role.name
}

# EC2 bastion instance
resource "aws_instance" "bastion" {
  ami           = "ami-0c7217cdde317cfec" # Ubuntu 22.04 in us-east-1
  instance_type = "t3.micro"
  key_name      = "msk-bastion-key"

  vpc_security_group_ids = [aws_security_group.bastion_sg.id]
  subnet_id              = local.public_subnet_ids[0]

  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.bastion_profile.name

  user_data = <<-EOF
              #!/bin/bash
              apt update -y
              apt install -y python3 python3-pip
              pip3 install kafka-python boto3
              EOF

  tags = {
    Name  = "msk-bastion"
    owner = "randy.pitcher@databricks.com"
  }
}

# Allow bastion to access MSK
# resource "aws_security_group_rule" "msk_from_bastion" {
#   type                     = "ingress"
#   from_port               = 9092
#   to_port                 = 9198
#   protocol                = "tcp"
#   source_security_group_id = aws_security_group.bastion_sg.id
#   security_group_id       = aws_security_group.msk_sg.id
# }

# Outputs
output "bastion_public_ip" {
  description = "Public IP of the bastion host"
  value       = aws_instance.bastion.public_ip
}

output "bastion_private_ip" {
  description = "Private IP of the bastion host"
  value       = aws_instance.bastion.private_ip
}

output "ssh_command" {
  description = "SSH command to connect to bastion"
  value       = "ssh ubuntu@${aws_instance.bastion.public_ip}"
} 