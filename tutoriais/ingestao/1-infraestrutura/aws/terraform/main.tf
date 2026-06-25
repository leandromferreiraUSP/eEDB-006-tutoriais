# main.tf -- Infraestrutura AWS para os Tutoriais 2 (Meltano) e 3 (DLTHub) na nuvem.
# Provisiona, no AWS Academy Learner Lab:
#   - S3 bucket (data lake de DESTINO)
#   - Security Group (SSH no EC2; Postgres EC2 -> RDS)
#   - RDS PostgreSQL (banco de ORIGEM)
#   - EC2 (roda a ferramenta de ingestão; usa LabInstanceProfile p/ gravar no S3)
#
# Tudo na default VPC do Lab. Region/recursos compatíveis com as restrições do Learner Lab.

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Subnets para a EC2: excluímos us-east-1e, onde tipos t3 não são suportados no Lab.
data "aws_subnets" "ec2" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1f"]
  }
}

# AMI mais recente do Amazon Linux 2023 (x86_64), via parâmetro público do SSM.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  bucket_name = "${data.aws_caller_identity.current.account_id}-ingestao-lab"
}

# ----------------------------------------------------------------- S3 (destino)
resource "aws_s3_bucket" "datalake" {
  bucket        = local.bucket_name
  force_destroy = true

  tags = {
    Name      = local.bucket_name
    Project   = "ingestao-lab"
    ManagedBy = "terraform"
  }
}

# ----------------------------------------------------------------- Security Group
resource "aws_security_group" "ingestao" {
  name        = "ingestao-sg"
  description = "SSH no EC2 e Postgres interno (EC2 para RDS)"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  # Postgres acessível somente de dentro do próprio SG (a EC2 alcança o RDS)
  ingress {
    description = "Postgres interno (EC2 para RDS)"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "ingestao-sg"
    Project = "ingestao-lab"
  }
}

# ----------------------------------------------------------------- RDS PostgreSQL (origem)
resource "aws_db_subnet_group" "ingestao" {
  name       = "ingestao-db-subnets"
  subnet_ids = data.aws_subnets.default.ids

  tags = {
    Name    = "ingestao-db-subnets"
    Project = "ingestao-lab"
  }
}

resource "aws_db_instance" "postgres" {
  identifier        = "ingestao-postgres"
  engine            = "postgres"
  instance_class    = var.db_instance_class
  allocated_storage = 20
  storage_type      = "gp2"

  db_name  = "ecommerce"
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.ingestao.name
  vpc_security_group_ids = [aws_security_group.ingestao.id]
  publicly_accessible    = false

  skip_final_snapshot     = true
  backup_retention_period = 0
  apply_immediately       = true

  tags = {
    Name    = "ingestao-postgres"
    Project = "ingestao-lab"
  }
}

# ----------------------------------------------------------------- EC2 (runner de ingestão)
resource "aws_instance" "runner" {
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.instance_type
  key_name                    = var.key_name
  iam_instance_profile        = var.lab_instance_profile
  subnet_id                   = data.aws_subnets.ec2.ids[0]
  vpc_security_group_ids      = [aws_security_group.ingestao.id]
  associate_public_ip_address = true

  # Prepara a máquina: git, cliente psql e Python (para Meltano/DLT).
  user_data = <<-EOF
    #!/bin/bash
    set -e
    dnf update -y
    dnf install -y git python3.11 python3.11-pip postgresql15
    EOF

  tags = {
    Name    = "ingestao-runner"
    Project = "ingestao-lab"
  }
}
