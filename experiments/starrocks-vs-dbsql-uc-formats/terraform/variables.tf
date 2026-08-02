variable "aws_region" {
  description = "AWS region for the StarRocks VM"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type for the StarRocks allin1 node"
  type        = string
  default     = "m5.2xlarge"
}

variable "starrocks_version" {
  description = "starrocks/allin1-ubuntu image tag. Pin an exact patch for reproducible results."
  type        = string
  default     = "4.0.13"
}

variable "allowed_ingress_cidrs" {
  description = <<-EOT
    CIDRs allowed to reach the FE MySQL port (9030) and FE/BE HTTP (8030/8040).
    No default on purpose: decide your own exposure. The FE root account runs
    with NO PASSWORD in this evaluation rig — lock these to your own egress IP
    for anything beyond a short-lived synthetic-data run.
  EOT
  type        = list(string)
}

variable "ssh_public_key" {
  description = <<-EOT
    Optional SSH public key material. When set (together with ssh_ingress_cidr)
    the VM gets a key pair and port 22, which is how you read StarRocks logs
    (`docker logs starrocks`) when FE/BE misbehave. Leave empty for no SSH.
  EOT
  type        = string
  default     = ""
}

variable "ssh_ingress_cidr" {
  description = "CIDR allowed to SSH. Use your own /32 — never widen this."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project   = "starrocks-vs-dbsql-uc-formats"
    ephemeral = "true"
  }
}
