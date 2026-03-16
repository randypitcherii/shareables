# MSK Terraform vs AWS Reality

_Last updated: 2025-07-29_

## 1. What Terraform Declares (kafka/terraform-msk-instance)

### Networking
- Default VPC (data source)`;'
- Brokers in hard-coded **public subnets**:
  - `subnet-07480b8fbbb9501bd`
  - `subnet-0a04badfd09d050b0`

### Security Groups
- **`msk_sg`** – Ingress **9092-9198** & **2181** to two /1 CIDRs (`0.0.0.0/1` + `128.0.0.0/1`) ⇒ effectively `0.0.0.0/0` but meets org policy.
- **`bastion_sg`** – SSH (22) open to same split CIDRs.

### KMS & Secrets
- Reuse existing KMS key for at-rest encryption.
- New KMS key for SCRAM secret.
- Secrets Manager secret **`AmazonMSK_randy_pitcher_workspace_mini_scram`** (username `silky_airplane`).

### MSK Configurations
- **initial** config: `allow.everyone.if.no.acl.found=true` (permissive).
- **final** config:   `allow.everyone.if.no.acl.found=false` (restrictive).

### MSK Cluster
- Name `randy-pitcher-workspace-mini`, 2 × `kafka.t3.small`.
- Authentication: SASL-SCRAM **and** IAM.
- Public access + config ARN toggled by var **`enable_public_access`**.

### Bastion Host
- EC2 **t3.micro** in public subnet, public IP.
- IAM role with `AmazonMSKFullAccess`.
- User-data installs only Python libs (Kafka CLI installed manually).

---

## 2. What Terraform Does **Not** Manage (but cluster needs)

| Need | How we do it now | Why manual? |
|------|------------------|-------------|
| Kafka ACLs for `silky_airplane` | Run `kafka-acls.sh …` from bastion | No Terraform resource yet |
| Kafka CLI & Java on bastion | SSH > install JDK & Kafka binaries | Not in user-data |
| Two-phase apply (private → ACLs → public) | Operator toggles `enable_public_access` | Terraform cannot express this ordering |
| Optional bastion teardown | `terraform destroy -target=aws_instance.bastion` | Operator choice |

---

## 3. Manual Deploy-from-Scratch Playbook

1. **Initial apply (private / permissive)**
   ```bash
   cd kafka/terraform-msk-instance
   terraform init
   terraform apply \
     -var=kafka_username="silky_airplane" \
     -var=kafka_password="<pwd>" \
     -var=enable_public_access=false
   ```
2. **SSH to bastion**
   ```bash
   ssh ubuntu@$(terraform output -raw bastion_public_ip)
   ```
   Install tooling:
   ```bash
   sudo apt update -y && sudo apt install -y openjdk-11-jre-headless wget unzip
   wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
   tar -xzf kafka_2.13-3.8.0.tgz && sudo mv kafka_2.13-3.8.0 /opt/kafka
   ```
   Grant ACLs:
   ```bash
   cat > /opt/kafka/config/client.properties <<EOF2
   bootstrap.servers=<private_brokers_sasl_scram>
   security.protocol=SASL_SSL
   sasl.mechanism=SCRAM-SHA-512
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="silky_airplane" password="<pwd>";
   EOF2

   /opt/kafka/bin/kafka-acls.sh \
     --bootstrap-server $BOOTSTRAP \
     --command-config /opt/kafka/config/client.properties \
     --add --allow-principal User:silky_airplane \
     --operation All --topic '*' --group '*' --cluster
   ```
3. **Switch to restrictive config + public endpoints**
   ```bash
   terraform apply -var=enable_public_access=true
   ```
4. **Validate locally** using `kafka/client-scram.properties`.
5. **(Optional) Destroy bastion** when no longer needed.

---

## 4. Networking Assumptions
- VPC + the two public subnets already exist and have Internet-gateway routes.
- Org blocks `0.0.0.0/0`; workaround is dual /1 CIDRs.

---

## 5. Future Automation Ideas
- Manage ACLs via provider once available.
- Pipeline: TF apply (private) → remote-exec on bastion for ACLs → TF apply (public).
- Bake Kafka CLI into AMI or extend user-data.
- Replace bastion with on-demand Lambda / Fargate task for ACL bootstrap.

