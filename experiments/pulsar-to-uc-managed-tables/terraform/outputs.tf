output "public_ip" {
  description = "Public IP of the Pulsar VM"
  value       = aws_instance.pulsar.public_ip
}

output "pulsar_service_url" {
  description = "Pulsar binary protocol URL (native clients + Databricks pulsar connector)"
  value       = "pulsar://${aws_instance.pulsar.public_ip}:6650"
}

output "pulsar_admin_url" {
  description = "Pulsar admin/HTTP API URL"
  value       = "http://${aws_instance.pulsar.public_ip}:8080"
}

output "kafka_bootstrap" {
  description = "Kafka protocol bootstrap servers (KoP)"
  value       = "${aws_instance.pulsar.public_ip}:9092"
}
