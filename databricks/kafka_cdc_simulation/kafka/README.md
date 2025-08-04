# Kafka MSK Setup Guide

This directory contains the infrastructure and configuration for setting up an Amazon MSK (Managed Streaming for Apache Kafka) cluster with proper authentication and access controls.

## Quick Setup Steps

### 1. Deploy MSK Infrastructure
```bash
cd terraform-msk-instance
terraform init
terraform apply -auto-approve -var="kafka_username=silky_airplane" -var="kafka_password=your_password"
```

### 2. Configure Bastion Host
The bastion host is automatically deployed with Terraform and provides SSH access for Kafka administration.

**SSH to bastion:**
```bash
ssh -i ~/.ssh/bastion-key.pem ubuntu@<bastion_public_ip>
```

**Install Kafka CLI tools on bastion:**
```bash
sudo apt update -y
sudo apt install -y openjdk-11-jre-headless wget unzip
wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
mv kafka_2.13-3.8.0 /home/ubuntu/kafka
```

### 3. Set Up Kafka ACLs
Configure client properties and grant admin permissions:

```bash
# Create client.properties with SCRAM credentials
cat > /home/ubuntu/kafka/config/client.properties << EOF
bootstrap.servers=b-1.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9096,b-2.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9096
sasl.mechanism=SCRAM-SHA-512
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="silky_airplane" password="your_password!";
EOF

# Grant admin ACLs
/home/ubuntu/kafka/bin/kafka-acls.sh --bootstrap-server b-1.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9096 --command-config /home/ubuntu/kafka/config/client.properties --add --allow-principal User:silky_airplane --operation All --topic '*' --group '*' --cluster
```

### 4. Enable ACLs and Public Access
Update the MSK cluster configuration to enable ACLs and public access:

```bash
# In terraform-msk-instance/
terraform apply -auto-approve -var="enable_public_access=true"
```

## Key Files

- `terraform-msk-instance/` - Terraform configuration for MSK cluster, bastion host, and security groups
- `client-scram.properties.template` - Template for SCRAM authentication configuration
- `client.properties` - IAM authentication configuration

## Authentication Methods

- **SCRAM**: Username/password authentication (configured for `silky_airplane` user)
- **IAM**: AWS IAM-based authentication for service-to-service communication

## Security Notes

- Bastion host has full MSK access via IAM role
- SCRAM credentials stored in AWS Secrets Manager
- Public access enabled for external connectivity
- ACLs configured to restrict access to authorized users only 