# ShadowTraffic EC2 Terraform Deployment

This directory contains the Terraform configuration for deploying ShadowTraffic to an AWS EC2 instance for continuous CDC event production.

## Overview

The deployment provisions:
- **EC2 Instance**: m5.2xlarge with Amazon Linux 2
- **Security Group**: Allows SSH access from anywhere
- **Docker Container**: ShadowTraffic running with auto-restart
- **Integration**: Connects to existing MSK cluster

## Prerequisites

1. **AWS Authentication**: Ensure you're authenticated to AWS
   ```bash
   aws_fe_sandbox
   ```

2. **SSH Key**: The `msk-bastion-key.pem` should exist in `~/.ssh/`

3. **Environment Variables**: Set the following variables (via `.env` file or environment):
   - `USERNAME`: Kafka SCRAM username (defaults to "silky_airplane")
   - `PASSWORD`: Kafka SCRAM password
   - `KAFKA_BROKERS`: MSK broker endpoints

4. **Files**: Ensure these files exist in the parent directory:
   - `license.env`: ShadowTraffic license file
   - `cdc_generator.json`: ShadowTraffic configuration

## Quick Deployment

### Option 1: Using the deployment script (Recommended)

```bash
cd databricks/kafka_cdc_simulation/kafka_producers/shadowtraffic/terraform
./deploy.sh
```

The script will:
- Check prerequisites
- Source environment variables
- Initialize Terraform
- Plan the deployment
- Ask for confirmation
- Apply the configuration
- Display connection information

### Option 2: Manual deployment

```bash
cd databricks/kafka_cdc_simulation/kafka_producers/shadowtraffic/terraform

# Set environment variables
export TF_VAR_username="your_username"
export TF_VAR_password="your_password"
export TF_VAR_kafka_brokers="your_brokers"

# Deploy
terraform init
terraform plan
terraform apply -auto-approve
```

## Monitoring

After deployment, you can monitor the ShadowTraffic container:

```bash
# SSH to the instance
ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@<public_ip>

# Check Docker container status
docker ps

# View ShadowTraffic logs
docker logs shadowtraffic

# Follow logs in real-time
docker logs -f shadowtraffic
```

## Configuration Details

### Instance Specifications
- **Type**: m5.2xlarge (8 vCPU, 32GB RAM)
- **OS**: Amazon Linux 2
- **Cost**: ~$0.384/hour in us-east-1

### Security
- **SSH Access**: Open to all IPs (0.0.0.0/1 and 128.0.0.0/1)
- **Outbound**: All traffic allowed
- **Key**: Reuses `msk-bastion-key` from MSK deployment

### Docker Configuration
- **Image**: `shadowtraffic/shadowtraffic:latest`
- **Restart Policy**: `unless-stopped`
- **Volume Mounts**: `cdc_generator.json` mounted as `/home/config.json`
- **Environment**: License file + injected secrets

## Environment Variables

The container receives these environment variables:

| Variable | Source | Purpose |
|----------|--------|---------|
| `KAFKA_TOPIC` | Hardcoded | Target topic (`dev_sturdy_chair`) |
| `KAFKA_BROKERS` | TF Variable | MSK broker endpoints |
| `KAFKA_SASL_JAAS_CONFIG` | TF Variables | SCRAM authentication |
| `RUN_STARTED_AT` | Generated | Deployment timestamp |
| `PAYLOAD_STRING` | Generated | 30KB payload (7680 🪐 characters) |

## Troubleshooting

### Common Issues

1. **SSH Connection Failed**
   - Verify SSH key exists: `ls -la ~/.ssh/msk-bastion-key.pem`
   - Check security group allows SSH (port 22)

2. **Docker Container Not Running**
   - SSH to instance and check: `docker ps -a`
   - View logs: `docker logs shadowtraffic`

3. **Kafka Connection Issues**
   - Verify broker endpoints are correct
   - Check SCRAM credentials
   - Ensure MSK cluster is accessible

4. **Environment Variables Missing**
   - Create `.env` file in project root with required variables
   - Or set them manually before running Terraform

### Useful Commands

```bash
# Check instance status
terraform show

# View outputs
terraform output

# Destroy deployment
terraform destroy

# Force recreate instance
terraform taint aws_instance.shadowtraffic_producer
terraform apply
```

## Cost Management

- **Instance Cost**: ~$0.384/hour (~$280/month)
- **Data Transfer**: Minimal (CDC events only)
- **Storage**: EBS volume included with instance

To minimize costs:
- Use `terraform destroy` when not needed
- Consider scheduling with AWS Systems Manager
- Monitor usage in AWS Cost Explorer

## Security Notes

- SSH access is wide open for convenience
- Consider restricting CIDR blocks for production
- License file is copied to instance but can be deleted post-deployment
- Secrets are passed via Terraform variables (not stored in state)

## Updates and Maintenance

To update the deployment:

1. **Configuration Changes**: Modify `cdc_generator.json` and redeploy
2. **Environment Variables**: Update `.env` file and redeploy
3. **Terraform Changes**: Modify Terraform files and run `terraform apply`

The container will automatically restart with new configuration. 