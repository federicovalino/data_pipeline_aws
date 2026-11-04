# ---------------------------------------------------------------------------
# Módulo storage: bucket del Data Lake con hardening base.
# ---------------------------------------------------------------------------

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
  }
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.name_prefix}-datalake-${data.aws_caller_identity.current.account_id}"
  tags   = { DataDomain = "lakehouse" }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

locals {
  # Siempre: rechazar acceso sin HTTPS
  deny_insecure_transport = {
    Sid       = "DenyInsecureTransport"
    Effect    = "Deny"
    Principal = "*"
    Action    = "s3:*"
    Resource  = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
    Condition = { Bool = { "aws:SecureTransport" = "false" } }
  }

  # Opcional: los DATOS (objetos) solo se leen/escriben desde la VPC.
  # Se restringe solo el data plane, así Terraform puede seguir
  # gestionando la configuración del bucket desde fuera de la VPC.
  #
  # Excepción explícita: servicios administrados que escriben desde fuera de
  # la VPC (ej. Kinesis Firehose) se listan en vpce_exempt_principal_arns.
  # Las condiciones de un Deny se combinan con AND: se deniega solo si el
  # acceso NO viene por el endpoint Y el principal NO está exceptuado.
  vpce_condition   = { StringNotEquals = { "aws:SourceVpce" = var.allowed_vpce_id } }
  exempt_condition = {
    for k, v in { ArnNotLike = { "aws:PrincipalArn" = var.vpce_exempt_principal_arns } } :
    k => v if length(var.vpce_exempt_principal_arns) > 0
  }

  deny_outside_vpce = {
    Sid       = "DenyObjectAccessOutsideVpce"
    Effect    = "Deny"
    Principal = "*"
    Action    = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    Resource  = "${aws_s3_bucket.data_lake.arn}/*"
    Condition = merge(local.vpce_condition, local.exempt_condition)
  }

  policy_statements = concat(
    [local.deny_insecure_transport],
    var.allowed_vpce_id == null ? [] : [local.deny_outside_vpce]
  )
}

resource "aws_s3_bucket_policy" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.policy_statements
  })

  depends_on = [aws_s3_bucket_public_access_block.data_lake]
}