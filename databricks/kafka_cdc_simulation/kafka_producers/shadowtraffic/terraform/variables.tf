# Sensitive variables for Kafka authentication
variable "username" { 
  description = "Kafka SCRAM username"
  sensitive = true 
  default   = "silky_airplane"  # Consistent with MSK docs
}

variable "password" { 
  description = "Kafka SCRAM password"
  sensitive = true 
}

variable "kafka_brokers" { 
  description = "Kafka broker endpoints"
  sensitive = true 
}

# Hardcoded values from MSK setup
variable "vpc_id" { 
  description = "VPC ID for the deployment"
  default = "vpc-00debc3a19d114cda"  # Actual default VPC ID from AWS
}

variable "subnets" { 
  description = "Subnet IDs for the deployment"
  default = ["subnet-07480b8fbbb9501bd", "subnet-0a04badfd09d050b0"]
} 