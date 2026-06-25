output "s3_bucket" {
  description = "Nome do bucket S3 (data lake de destino)."
  value       = aws_s3_bucket.datalake.bucket
}

output "ec2_public_ip" {
  description = "IP público da EC2 runner (use no SSH)."
  value       = aws_instance.runner.public_ip
}

output "ec2_public_dns" {
  description = "DNS público da EC2 runner."
  value       = aws_instance.runner.public_dns
}

output "ssh_command" {
  description = "Comando de SSH pronto (ajuste o caminho do .pem se necessário)."
  value       = "ssh -i ~/.ssh/labsuser.pem ec2-user@${aws_instance.runner.public_ip}"
}

output "rds_endpoint" {
  description = "Endereço (host) do RDS Postgres."
  value       = aws_db_instance.postgres.address
}

output "rds_port" {
  description = "Porta do RDS Postgres."
  value       = aws_db_instance.postgres.port
}

output "db_name" {
  description = "Nome do banco no RDS."
  value       = aws_db_instance.postgres.db_name
}
