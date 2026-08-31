output "public_ip" {
  description = "Public IP of the StarRocks VM"
  value       = aws_instance.starrocks.public_ip
}

output "starrocks_fe_mysql" {
  description = "FE MySQL protocol endpoint (host:port for mysql/pymysql clients)"
  value       = "${aws_instance.starrocks.public_ip}:9030"
}

output "starrocks_fe_http" {
  description = "FE HTTP endpoint"
  value       = "http://${aws_instance.starrocks.public_ip}:8030"
}
