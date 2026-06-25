# Tutorial 1 (AWS): Infraestrutura de Ingestão na Nuvem com Terraform

> Versão **longa e explicativa**. Aqui você provisiona, na **AWS** (AWS Academy Learner Lab),
> a mesma topologia do tutorial local, porém com serviços gerenciados: um **RDS PostgreSQL**
> (origem), um **bucket S3** (data lake de destino) e uma **EC2** que roda a ferramenta de
> ingestão (Tutoriais 2 e 3 na versão AWS). Tudo via **Terraform** (infraestrutura como código).
>
> Só os comandos? Veja o `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Arquitetura na AWS](#1-arquitetura-na-aws)
2. [O ambiente: AWS Academy Learner Lab](#2-o-ambiente-aws-academy-learner-lab)
3. [Pré-requisitos por sistema operacional](#3-pré-requisitos-por-sistema-operacional)
4. [Credenciais do Learner Lab](#4-credenciais-do-learner-lab)
5. [Entendendo o Terraform deste tutorial](#5-entendendo-o-terraform-deste-tutorial)
6. [Provisionando a infraestrutura](#6-provisionando-a-infraestrutura)
7. [Populando o RDS (seed)](#7-populando-o-rds-seed)
8. [Validando](#8-validando)
9. [Plano B: RDS bloqueado no Lab](#9-plano-b-rds-bloqueado-no-lab)
10. [Destruindo tudo (evite custos!)](#10-destruindo-tudo-evite-custos)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Arquitetura na AWS

```
            ┌──────────────────────────── AWS (us-east-1) ───────────────────────────┐
            │                          default VPC                                    │
            │                                                                         │
  você ──SSH(22)──►  ┌──────────────────┐    psql(5432)    ┌──────────────────────┐  │
  (labsuser.pem)     │   EC2 runner     │  ─────────────►  │  RDS PostgreSQL       │  │  ORIGEM
                     │   Meltano / DLT  │                  │  ingestao-postgres    │  │
                     │  LabInstanceProf │                  │  db: ecommerce        │  │
                     └────────┬─────────┘                  └──────────────────────┘  │
            │                 │ s3:PutObject (via IAM role do Lab)                    │
            │                 ▼                                                       │
            │        ┌──────────────────────┐                                        │  DESTINO
            │        │  S3  <conta>-ingestao-lab │                                    │  (data lake)
            │        └──────────────────────┘                                        │
            └─────────────────────────────────────────────────────────────────────────┘
```

Comparando com o **Tutorial 1 Local**:

| Local (Docker) | AWS (este tutorial) |
|---|---|
| Container `postgres:16` | **Amazon RDS** PostgreSQL |
| Container MiniStack (S3 :4566) | **Amazon S3** (bucket real) |
| Ferramenta roda na sua máquina | Ferramenta roda na **EC2** |
| `docker compose up` | `terraform apply` |

---

## 2. O ambiente: AWS Academy Learner Lab

Este tutorial assume o **AWS Academy Learner Lab**. Restrições que moldam o Terraform:

| Restrição | Valor | Como tratamos |
|---|---|---|
| Região | `us-east-1` | fixada nas variáveis |
| Key pair | `vockey` (arquivo `labsuser.pem`) | `var.key_name = "vockey"` |
| IAM | use as roles pré-criadas (`LabRole` / `LabInstanceProfile`) | EC2 usa `LabInstanceProfile` |
| Credenciais | temporárias (expiram, ~3–4h) | reabra o Lab e recopie quando expirar |
| Instâncias | até `large` | EC2 `t3.small`, RDS `db.t3.micro` |

> ⚠️ **Custos**: RDS e EC2 são **cobrados por hora** enquanto ligados. Faça o laboratório e
> **destrua** ao final (seção 10). O orçamento do Lab é limitado.

---

## 3. Pré-requisitos por sistema operacional

Você precisa do **Terraform** e do **AWS CLI v2**.

### 3.1 — macOS

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform awscli
terraform -version && aws --version
```

### 3.2 — Linux (Ubuntu)

```bash
# Terraform (repositório HashiCorp):
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform

# AWS CLI v2:
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip
terraform -version && aws --version
```

### 3.3 — Windows (PowerShell)

```powershell
winget install -e --id Hashicorp.Terraform
winget install -e --id Amazon.AWSCLI
# feche e reabra o PowerShell, então:
terraform -version
aws --version
```

---

## 4. Credenciais do Learner Lab

As credenciais ficam em **`tutoriais/aws_credenciais/`**: `credentials`, `config` e a chave
SSH `labsuser.pem`. Copie-as para os locais padrão (`~/.aws/` e `~/.ssh/`).

> Esse passo é o mesmo do tutorial `install_aws_pre_req_tutorial`. Reabra o Learner Lab e
> recopie `credentials` sempre que a sessão expirar (as chaves mudam a cada sessão).

### 4.1 — macOS / Linux

```bash
mkdir -p ~/.aws ~/.ssh
cp tutoriais/aws_credenciais/credentials ~/.aws/credentials
cp tutoriais/aws_credenciais/config      ~/.aws/config
cp tutoriais/aws_credenciais/labsuser.pem ~/.ssh/labsuser.pem
chmod 600 ~/.aws/credentials ~/.ssh/labsuser.pem
```

### 4.2 — Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.aws" | Out-Null
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.ssh" | Out-Null
$proj = "C:\caminho\para\Big Data\tutoriais\aws_credenciais"   # ajuste
Copy-Item "$proj\credentials"  "$env:USERPROFILE\.aws\credentials" -Force
Copy-Item "$proj\config"       "$env:USERPROFILE\.aws\config" -Force
Copy-Item "$proj\labsuser.pem" "$env:USERPROFILE\.ssh\labsuser.pem" -Force
```

Valide a identidade:

```bash
aws sts get-caller-identity
```

**Resultado esperado** (algo como):

```json
{
  "Account": "849967252385",
  "Arn": "arn:aws:sts::849967252385:assumed-role/voclabs/user..."
}
```

---

## 5. Entendendo o Terraform deste tutorial

Os arquivos estão em `1-infraestrutura/aws/terraform/`:

| Arquivo | O que define |
|---|---|
| `versions.tf` | provider `aws ~> 5.0`, região |
| `variables.tf` | região, tipos de instância, usuário/senha do banco, `vockey`, `LabInstanceProfile` |
| `main.tf` | S3 + Security Group + RDS Postgres + EC2 |
| `outputs.tf` | IP da EC2, endpoint do RDS, nome do bucket |

Trechos-chave do `main.tf` (leia o arquivo completo):

```hcl
locals {
  bucket_name = "${data.aws_caller_identity.current.account_id}-ingestao-lab"
}

resource "aws_db_instance" "postgres" {
  identifier             = "ingestao-postgres"
  engine                 = "postgres"
  instance_class         = var.db_instance_class      # db.t3.micro
  allocated_storage      = 20
  db_name                = "ecommerce"
  username               = var.db_username
  password               = var.db_password
  vpc_security_group_ids = [aws_security_group.ingestao.id]
  publicly_accessible    = false                      # acesso só pela EC2 (dentro da VPC)
  skip_final_snapshot    = true
}

resource "aws_instance" "runner" {
  ami                  = data.aws_ssm_parameter.al2023.value   # Amazon Linux 2023
  instance_type        = var.instance_type
  key_name             = var.key_name                 # vockey
  iam_instance_profile = var.lab_instance_profile     # LabInstanceProfile -> acesso ao S3
  # ...
}
```

> **Por que não pinamos `engine_version` do RDS?** Para a AWS escolher uma versão 16.x válida
> e disponível na criação, evitando erro de "versão inexistente" se um minor for descontinuado.

---

## 6. Provisionando a infraestrutura

Na pasta do Terraform:

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform init
terraform plan        # confira: 5 recursos a criar
terraform apply       # digite "yes" para confirmar
```

> Opcional: defina uma senha própria do banco com `terraform apply -var="db_password=MinhaSenhaForte123"`.

A criação do **RDS leva ~5–10 minutos**. Ao final, o Terraform imprime os **outputs**:

**Resultado esperado** (valores variam):

```
Outputs:
db_name        = "ecommerce"
ec2_public_ip  = "54.x.x.x"
rds_endpoint   = "ingestao-postgres.xxxxx.us-east-1.rds.amazonaws.com"
rds_port       = 5432
s3_bucket      = "849967252385-ingestao-lab"
ssh_command    = "ssh -i ~/.ssh/labsuser.pem ec2-user@54.x.x.x"
```

Guarde esses valores — você vai usá-los no seed e nos Tutoriais 2 e 3 (AWS). Pode relê-los a
qualquer momento com:

```bash
terraform output
terraform output -raw rds_endpoint
```

---

## 7. Populando o RDS (seed)

O RDS sobe **vazio**. Como ele **não é público** (só acessível de dentro da VPC), o seed roda
**a partir da EC2**. O fluxo é: enviar os SQLs para a EC2 → conectar nela → rodar `psql`
contra o RDS.

### 7.1 — Enviar os SQLs para a EC2

Da sua máquina, na raiz do projeto:

```bash
EC2=$(cd tutoriais/ingestao/1-infraestrutura/aws/terraform && terraform output -raw ec2_public_ip)
scp -i ~/.ssh/labsuser.pem \
  tutoriais/ingestao/dados/schema.sql \
  tutoriais/ingestao/dados/seed.sql \
  ec2-user@$EC2:/home/ec2-user/
```

No **Windows (PowerShell)**, o `scp` também existe (OpenSSH nativo); use o IP do output:

```powershell
scp -i $env:USERPROFILE\.ssh\labsuser.pem `
  tutoriais\ingestao\dados\schema.sql tutoriais\ingestao\dados\seed.sql `
  ec2-user@SEU_IP_EC2:/home/ec2-user/
```

### 7.2 — Conectar na EC2 e rodar o seed

```bash
ssh -i ~/.ssh/labsuser.pem ec2-user@$EC2
```

Já dentro da EC2 (o cliente `psql` foi instalado pelo `user_data`), defina o host do RDS e a
senha, e rode os scripts:

```bash
RDS=ingestao-postgres.xxxxx.us-east-1.rds.amazonaws.com   # use seu rds_endpoint
export PGPASSWORD=ecommerce123                            # use sua db_password

psql -h $RDS -U ecommerce -d ecommerce -f schema.sql
psql -h $RDS -U ecommerce -d ecommerce -f seed.sql
```

**Resultado esperado**: várias linhas `INSERT 0 1` e, ao final, `COMMIT`.

---

## 8. Validando

Ainda na EC2, confira as contagens no RDS:

```bash
psql -h $RDS -U ecommerce -d ecommerce -c \
  "SELECT 'clientes' t, count(*) FROM clientes
   UNION ALL SELECT 'produtos', count(*) FROM produtos
   UNION ALL SELECT 'vendas',   count(*) FROM vendas;"
```

**Resultado esperado**: `clientes=20`, `produtos=15`, `vendas=200`.

Da **sua máquina**, confira o bucket S3 (ainda vazio — os dados chegam nos Tutoriais 2/3):

```bash
aws s3 ls s3://$(cd tutoriais/ingestao/1-infraestrutura/aws/terraform && terraform output -raw s3_bucket)/
```

> No S3 **real** você **não** precisa de `--endpoint-url` nem do `AWS_REQUEST_CHECKSUM_CALCULATION`
> (aquilo era específico do MiniStack local).

A infraestrutura AWS está pronta. Siga para `2-meltano/aws/` ou `3-dlthub/aws/`. **Deixe a EC2
e o RDS ligados** enquanto faz esses tutoriais — e lembre de destruir no fim.

---

## 9. Plano B: RDS bloqueado no Lab

Alguns Learner Labs **bloqueiam o RDS** por política (SCP). Se o `terraform apply` falhar na
criação do RDS com erro de permissão (ex.: `not authorized to perform: rds:CreateDBInstance`),
use o **Postgres em Docker na própria EC2** como origem — o resto (S3, EC2, Tutoriais 2/3)
permanece igual, mudando apenas o host do banco para `localhost` na EC2.

1. Comente/remova os recursos `aws_db_instance.postgres` e `aws_db_subnet_group.ingestao` do
   `main.tf` e o `engine`/seed do RDS, e rode `terraform apply` de novo (cria só S3 + SG + EC2).
2. Conecte na EC2 e suba um Postgres em container, já populado:

   ```bash
   sudo dnf install -y docker && sudo systemctl enable --now docker
   # envie dados/schema.sql e dados/seed.sql para ~/ (via scp, como na seção 7.1)
   sudo docker run -d --name pg -p 5432:5432 \
     -e POSTGRES_USER=ecommerce -e POSTGRES_PASSWORD=ecommerce -e POSTGRES_DB=ecommerce \
     -v /home/ec2-user/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro \
     -v /home/ec2-user/seed.sql:/docker-entrypoint-initdb.d/02-seed.sql:ro \
     postgres:16
   ```
3. Nos Tutoriais 2/3 (AWS), use `host=localhost` (o banco roda na própria EC2) em vez do
   endpoint do RDS.

---

## 10. Destruindo tudo (evite custos!)

Ao terminar (ou ao fim do dia de estudo):

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform destroy     # digite "yes"
```

> `force_destroy = true` no bucket faz o Terraform apagar os objetos do S3 junto. Confirme no
> console que **RDS** e **EC2** sumiram. Se o Lab encerrar a sessão antes do `destroy`, os
> recursos são derrubados automaticamente no fim do Lab — mas não conte com isso.

---

## 11. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ExpiredToken` / `InvalidClientTokenId` | Credenciais do Lab expiraram | Reabra o Lab, recopie `credentials` para `~/.aws/` |
| `UnauthorizedOperation` / `rds:CreateDBInstance` negado | RDS bloqueado no Lab | Use o **Plano B** (seção 9) |
| `ingress... doesn't comply with restrictions` | Caractere inválido na descrição do SG | Evite `>` `<` etc. nas descrições |
| SSH `Permission denied (publickey)` | `.pem` errado ou usuário errado | Use `ec2-user@IP` e `~/.ssh/labsuser.pem` (chmod 600) |
| `psql: could not connect ... timeout` ao RDS | Rodando da sua máquina (RDS não é público) | Rode o `psql` **de dentro da EC2** |
| `terraform apply` trava em "Still creating... [RDS]" | RDS demora mesmo | Aguarde ~5–10 min |
| `Unsupported: ... instance type ... not supported in ... us-east-1e` | Algumas AZs não têm os tipos `t3` | Já tratamos: o EC2 usa `data.aws_subnets.ec2`, que exclui `us-east-1e`. Se mudar de região/tipo, ajuste a lista de AZs |
| `Invalid security group description` | Descrição do SG com caractere proibido (ex.: `>`) | Evite `>`/`<`; use texto como `(EC2 para RDS)` |
| Esqueci os outputs | — | `terraform output` na pasta do terraform |
