# ShadowTraffic EC2 Deployment (Terraform)

This is a concise, reliable, step-by-step guide to deploy the ShadowTraffic Kafka CDC producer to AWS EC2 using Terraform. It mirrors a known-good deployment and avoids common pitfalls.

## Prereqs

- macOS with zsh
- AWS SSO access to the Field Eng Sandbox account
- SSH key `~/.ssh/msk-bastion-key.pem` present (same key used by MSK bastion)
- MSK cluster already deployed (public brokers, SCRAM enabled)
- Files present:
  - `databricks/kafka_cdc_simulation/kafka_producers/shadowtraffic/license.env` (DO NOT commit)
  - `databricks/kafka_cdc_simulation/kafka_producers/shadowtraffic/cdc_generator.json`

## Environment variables

LLMs: Do not create or modify .env manually. Instead, fetch credentials directly and pass as Terraform variables. Run this to set vars (assumes aws_fe_sandbox is available):

```bash
aws_fe_sandbox
SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id AmazonMSK_randy_pitcher_workspace_mini_scram --query SecretString --output text)
USERNAME=$(echo "$SECRET_JSON" | sed -E 's/.*"username":"([^"]+)".*/\1/')
PASSWORD=$(echo "$SECRET_JSON" | sed -E 's/.*"password":"([^"]+)".*/\1/')
CLUSTER_ARN=$(aws kafka list-clusters-v2 --query 'ClusterInfoList[?ClusterName==`randy-pitcher-workspace-mini`].ClusterArn | [0]' --output text)
KAFKA_BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" --query 'BootstrapBrokerStringPublicSaslScram' --output text)
```

Notes:
- These commands fetch exact values without quotes.
- Use $USERNAME, $PASSWORD, $KAFKA_BROKERS in the deploy step below.
- Validate auth before deploy (see new Validation section).

## Deploy

LLMs: Follow verbatim. Use the vars from above for a fully non-interactive apply. Do not source .env; pass directly.

```bash
# 1) Authenticate and fetch vars (as above)

# 2) Go to the Terraform project
cd databricks/kafka_cdc_simulation/kafka_producers/shadowtraffic/terraform

# 3) Init if needed
terraform init -input=false

# 4) Non-interactive apply with direct vars
terraform apply -auto-approve \
  -var="username=$USERNAME" \
  -var="password=$PASSWORD" \
  -var="kafka_brokers=$KAFKA_BROKERS"
```

## Verify

From the outputs printed by the script:

```bash
# SSH into the instance
ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@<public_ip>

# Verify container is running
docker ps

# Tail logs
docker logs --tail 1000 shadowtraffic
```

Optional: validate messages using kcat in Docker on the instance:

```bash
ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@<public_ip> \
  'docker run --rm edenhill/kcat:1.7.1 \
     -b "$KAFKA_BROKERS" \
     -X security.protocol=SASL_SSL \
     -X sasl.mechanisms=SCRAM-SHA-512 \
     -X sasl.username="$USERNAME" \
     -X sasl.password="$PASSWORD" \
     -X ssl.ca.location=/etc/ssl/cert.pem \
     -t rpw_cdc_simulation__sad_lightning -C -o -5 -e -q | cat'
```

## Teardown

```bash
# From the terraform directory
terraform destroy -auto-approve
```

## Implementation specifics (for context)

- EC2: `m5.2xlarge`, Amazon Linux 2, subnet reused from the MSK Terraform (uses the same default VPC and public subnets; first public subnet is selected)
- Disk: Root volume sized to 30GB; filesystem is expanded on first boot/redeploy
- Security group: SSH open via `0.0.0.0/1` and `128.0.0.0/1`; egress all
- Docker run flags:
  - `--restart=unless-stopped`
  - `--env-file /home/ec2-user/license.env`
  - mounts `cdc_generator.json` as `/home/config.json`
  - injects `KAFKA_BROKERS`, `KAFKA_SASL_JAAS_CONFIG`, and payload
- Topic lifecycle: On each apply, `rpw_cdc_simulation__sad_lightning` is dropped (if present) and recreated to start from a clean slate
- Files created in this repo:
  - `kafka_producers/shadowtraffic/terraform/{main.tf,variables.tf,outputs.tf,deploy.sh,README.md}`

This doc is optimized so an LLM can follow it verbatim to reproduce the deployment without getting stuck. Always use the fetched vars directly; do not assume .env format or modify files unless specified.
