output "cluster_arn" {
  description = "ARN of the MSK cluster"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.arn
}

output "cluster_name" {
  description = "Name of the MSK cluster"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.cluster_name
}

output "bootstrap_brokers" {
  description = "Bootstrap brokers for the MSK cluster"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers
}

output "bootstrap_brokers_sasl_scram" {
  description = "Bootstrap brokers for SASL SCRAM authentication"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers_sasl_scram
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap brokers for SASL IAM authentication"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers_sasl_iam
}

output "bootstrap_brokers_tls" {
  description = "Bootstrap brokers for TLS authentication"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers_tls
}

output "bootstrap_brokers_public_sasl_scram" {
  description = "Public bootstrap brokers for SASL SCRAM authentication"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers_public_sasl_scram
}

output "bootstrap_brokers_public_sasl_iam" {
  description = "Public bootstrap brokers for SASL IAM authentication"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers_public_sasl_iam
}

output "bootstrap_brokers_public_tls" {
  description = "Public bootstrap brokers for TLS authentication"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.bootstrap_brokers_public_tls
}

output "zookeeper_connect_string" {
  description = "ZooKeeper connect string"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.zookeeper_connect_string
}

output "zookeeper_connect_string_tls" {
  description = "ZooKeeper connect string with TLS"
  value       = aws_msk_cluster.randy_pitcher_workspace_mini.zookeeper_connect_string_tls
}

output "initial_configuration_arn" {
  description = "ARN of the initial MSK configuration"
  value       = aws_msk_configuration.msk_configuration_initial.arn
}

output "final_configuration_arn" {
  description = "ARN of the final MSK configuration"
  value       = aws_msk_configuration.msk_configuration_final.arn
}

output "existing_secret_arn" {
  description = "ARN of the existing SCRAM secret"
  value       = aws_secretsmanager_secret.msk_scram_secret.arn
}
