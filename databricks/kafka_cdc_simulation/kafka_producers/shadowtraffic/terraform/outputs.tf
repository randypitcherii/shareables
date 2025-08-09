output "instance_id" {
  description = "ID of the ShadowTraffic EC2 instance"
  value       = aws_instance.shadowtraffic_producer.id
}

output "public_ip" {
  description = "Public IP address of the ShadowTraffic EC2 instance"
  value       = aws_instance.shadowtraffic_producer.public_ip
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@${aws_instance.shadowtraffic_producer.public_ip}"
}

output "docker_logs_command" {
  description = "Command to view ShadowTraffic Docker logs"
  value       = "ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@${aws_instance.shadowtraffic_producer.public_ip} 'docker logs shadowtraffic'"
}

output "docker_status_command" {
  description = "Command to check Docker container status"
  value       = "ssh -i ~/.ssh/msk-bastion-key.pem ec2-user@${aws_instance.shadowtraffic_producer.public_ip} 'docker ps'"
} 