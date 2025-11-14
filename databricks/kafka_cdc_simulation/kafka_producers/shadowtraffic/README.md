# ShadowTraffic CDC Producer

This directory houses the **ShadowTraffic** configuration and deployment assets used to simulate high-volume CDC workloads into Amazon MSK.

## 📌 Key Points (2025-08-10 refresh)

1. **Topic management**  
   • Terraform no longer deletes / recreates the topic – ShadowTraffic now creates it on demand.  
   • The hard-coded `rpw_cdc_simulation__sad_lightning` topic remains the default; override with `KAFKA_TOPIC`.

2. **Phase-1 insert override**  
   • The generator uses `weightedOneOf` with `100 / 0` weights to force **100 % INSERTS** for the first 190 000 rows.  
   • Phase 2 falls back to 1.2 % INSERT / 98.8 % UPDATE.

3. **Large payload string**  
   • `PAYLOAD_STRING` is a 30 KB test payload built from 7 680 🌞 emojis.  
   • ShadowTraffic passes it unchanged; downstream consumers can validate large-message handling.

4. **Manual restart cheat-sheet**

**⚠️ IMPORTANT**: Use credentials from `databricks/kafka_cdc_simulation/kafka/client-scram.properties`, NOT from AWS Secrets Manager.

```bash
# From project root - extract credentials from client-scram.properties
PROPS_FILE="databricks/kafka_cdc_simulation/kafka/client-scram.properties"
USERNAME=$(grep '^username=' "$PROPS_FILE" | cut -d= -f2)
PASSWORD=$(grep '^password=' "$PROPS_FILE" | cut -d= -f2)
KAFKA_BROKERS=$(grep '^bootstrap.servers=' "$PROPS_FILE" | cut -d= -f2)

# Copy updated config
scp -i ~/.ssh/msk-bastion-key.pem \
    databricks/kafka_cdc_simulation/kafka_producers/shadowtraffic/cdc_generator.json \
    ec2-user@<EC2_PUBLIC_IP>:/home/ec2-user/cdc_generator.json

# Restart container with correct credentials
ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@<EC2_PUBLIC_IP> <<'EOF'
  PAYLOAD_STRING="$(printf '🌞%.0s' {1..7680})"
  sudo docker stop shadowtraffic || true
  sudo docker rm shadowtraffic   || true
  sudo docker run -d --name shadowtraffic --restart=unless-stopped \
    --env-file /home/ec2-user/license.env \
    -e KAFKA_TOPIC='rpw_cdc_simulation__sad_lightning' \
    -e KAFKA_BROKERS="$KAFKA_BROKERS" \
    -e KAFKA_SASL_JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$USERNAME\" password=\"$PASSWORD\";" \
    -e RUN_STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')" \
    -e PAYLOAD_STRING="$PAYLOAD_STRING" \
    -v /home/ec2-user/cdc_generator.json:/home/config.json \
    shadowtraffic/shadowtraffic:latest \
    --config /home/config.json
EOF
```

5. **Monitoring**

```bash
ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@<EC2_PUBLIC_IP>
# Tail logs
sudo docker logs -f --tail 50 shadowtraffic
```

---
*Last updated: 2025-08-12 - Fixed credential source documentation and emoji consistency.*

