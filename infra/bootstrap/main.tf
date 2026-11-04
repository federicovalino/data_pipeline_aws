# ---------------------------------------------------------------------------
# Bootstrap del backend remoto: crea el bucket del state y la tabla de locks.
# Es un root module independiente, con su propio state.
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5.0"

  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project_name
      Component = "terraform-backend"
      ManagedBy = "terraform"
    }
  }
}

# Lee el ID de tu cuenta para armar un nombre de bucket único sin hardcodearlo
data "aws_caller_identity" "current" {}

locals {
  state_bucket_name = "${var.project_name}-tfstate-${data.aws_caller_identity.current.account_id}"
  lock_table_name   = "${var.project_name}-tf-locks"
}

# --- Bucket del state ---------------------------------------------------------
resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket_name

  # Protege contra un "terraform destroy" accidental:
  # perder este bucket es perder la memoria de toda la infraestructura.
  lifecycle {
    prevent_destroy = true
  }
}

# Versionado: cada escritura del state queda como versión recuperable
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Cifrado del lado del servidor (SSE): lo pide la letra
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Nadie puede hacer público este bucket, ni por error
resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Rechaza cualquier acceso que no use HTTPS
resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.state]
}

# --- Tabla de locks -----------------------------------------------------------
# Terraform escribe un ítem con clave "LockID" mientras corre plan/apply,
# y lo borra al terminar. Si el ítem existe, otro apply tiene que esperar.
resource "aws_dynamodb_table" "locks" {
  name         = local.lock_table_name
  billing_mode = "PAY_PER_REQUEST" # pagás por operación: centavos para este uso
  hash_key     = "LockID"          # nombre exacto que espera Terraform

  attribute {
    name = "LockID"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}