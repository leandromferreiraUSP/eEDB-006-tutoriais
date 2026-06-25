variable "aws_region" {
  description = "Região AWS (Learner Lab: us-east-1)."
  type        = string
  default     = "us-east-1"
}

variable "ssh_cidr" {
  description = "CIDR autorizado a fazer SSH no EC2. Para restringir ao seu IP use \"SEU.IP.AQUI/32\"."
  type        = string
  default     = "0.0.0.0/0"
}

variable "instance_type" {
  description = "Tipo da instância EC2 que roda a ferramenta de ingestão (Learner Lab: até large)."
  type        = string
  default     = "t3.small"
}

variable "db_instance_class" {
  description = "Classe da instância RDS Postgres."
  type        = string
  default     = "db.t3.micro"
}

variable "db_username" {
  description = "Usuário master do Postgres (RDS)."
  type        = string
  default     = "ecommerce"
}

variable "db_password" {
  description = "Senha master do Postgres (RDS). Troque por algo seu; ambiente de laboratório."
  type        = string
  default     = "ecommerce123"
  sensitive   = true
}

variable "key_name" {
  description = "Key pair para SSH. No AWS Academy Learner Lab é \"vockey\" (arquivo labsuser.pem)."
  type        = string
  default     = "vockey"
}

variable "lab_instance_profile" {
  description = "Instance profile IAM pré-criado do Learner Lab (dá ao EC2 acesso ao S3)."
  type        = string
  default     = "LabInstanceProfile"
}
