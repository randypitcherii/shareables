# MSK Authentication Guide

## Quick Setup for MSK Connectivity

### 1. AWS CLI Authentication
Before any AWS operations, authenticate using:
```bash
aws_fe_sandbox
```
This sets up SSO authentication and AWS environment variables. You'll see the prompt change to indicate AWS authentication status.

### 2. MSK SCRAM Credentials
Credentials are stored in AWS Secrets Manager:
- **Secret Name:** `AmazonMSK_randy_pitcher_workspace_mini_scram`
- **Username:** `silky_airplane`
- **Password:** Retrieved from secret (auto-populated in client config)

To retrieve credentials:
```bash
aws secretsmanager get-secret-value --secret-id "AmazonMSK_randy_pitcher_workspace_mini_scram" --query 'SecretString' --output text
```

### 3. Public Bootstrap Brokers
- **SCRAM Endpoint:** `b-1-public.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9196,b-2-public.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9196`

### 4. Client Configuration
Use the pre-configured file: `kafka/client-scram.properties`

This file contains:
- Security protocol: `SASL_SSL`
- SASL mechanism: `SCRAM-SHA-512`
- JAAS config with embedded credentials

### 5. Basic Commands

**List topics:**
```bash
kafka-topics --bootstrap-server b-1-public.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9196,b-2-public.randypitcherworkspace.g6ltw7.c20.kafka.us-east-1.amazonaws.com:9196 --command-config kafka/client-scram.properties --list
```

**Create topic:**
```bash
kafka-topics --bootstrap-server <BROKERS> --command-config kafka/client-scram.properties --create --topic <TOPIC_NAME> --partitions 1 --replication-factor 1
```

**Produce message:**
```bash
echo "test message" | kafka-console-producer --bootstrap-server <BROKERS> --producer.config kafka/client-scram.properties --topic <TOPIC_NAME>
```

**Consume messages:**
```bash
kafka-console-consumer --bootstrap-server <BROKERS> --consumer.config kafka/client-scram.properties --topic <TOPIC_NAME> --from-beginning
```

### 6. Validation Commands
Test full CRUD permissions with these commands (replace `<BROKERS>` with the bootstrap brokers above).

## Notes
- MSK cluster has public access enabled
- SCRAM authentication is required (no unauthenticated access)
- Cluster is in `us-east-1` region
- Full CRUD permissions validated for the `silky_airplane` user