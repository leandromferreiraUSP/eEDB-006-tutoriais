# Quick Tutorial 1 (AWS): Infraestrutura com Terraform

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: RDS Postgres (origem) + S3 (destino) + EC2 (runner) provisionados no Learner Lab.

---

## 1. Pré-requisitos (uma vez)

- **macOS**: `brew install hashicorp/tap/terraform awscli`
- **Ubuntu**: Terraform (repo HashiCorp) + AWS CLI v2
- **Windows**: `winget install Hashicorp.Terraform Amazon.AWSCLI`

## 2. Credenciais do Lab → `~/.aws` e `~/.ssh`

```bash
mkdir -p ~/.aws ~/.ssh
cp tutoriais/aws_credenciais/credentials ~/.aws/credentials
cp tutoriais/aws_credenciais/config      ~/.aws/config
cp tutoriais/aws_credenciais/labsuser.pem ~/.ssh/labsuser.pem
chmod 600 ~/.aws/credentials ~/.ssh/labsuser.pem
aws sts get-caller-identity     # deve retornar a conta do Lab
```

(Windows: `Copy-Item` para `$env:USERPROFILE\.aws` e `\.ssh`.)

## 3. Provisionar

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform init
terraform plan        # 5 recursos
terraform apply       # yes  (RDS leva ~5-10 min)
terraform output      # ec2_public_ip, rds_endpoint, s3_bucket
```

## 4. Seed do RDS (a partir da EC2)

```bash
EC2=$(terraform output -raw ec2_public_ip)
RDS=$(terraform output -raw rds_endpoint)

# enviar os SQLs para a EC2:
scp -i ~/.ssh/labsuser.pem ../../../dados/schema.sql ../../../dados/seed.sql ec2-user@$EC2:/home/ec2-user/

# conectar e rodar:
ssh -i ~/.ssh/labsuser.pem ec2-user@$EC2
#   (dentro da EC2)
export PGPASSWORD=ecommerce123
psql -h <rds_endpoint> -U ecommerce -d ecommerce -f schema.sql
psql -h <rds_endpoint> -U ecommerce -d ecommerce -f seed.sql
psql -h <rds_endpoint> -U ecommerce -d ecommerce -c "SELECT count(*) FROM vendas;"   # 200
```

## 5. Destruir (ao final — evita custos!)

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform destroy     # yes
```

> RDS bloqueado no Lab? Veja o **Plano B** (seção 9 do `TUTORIAL.md`): Postgres em Docker na EC2.
