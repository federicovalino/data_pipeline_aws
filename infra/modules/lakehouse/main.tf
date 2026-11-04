# ---------------------------------------------------------------------------
# Módulo lakehouse: capa de almacenamiento y catálogo (pre-entrega 5).
#
#   S3  <prefix>-lakehouse-<account>
#     ├── warehouse/lakehouse_db.db/<tabla>/data/      archivos Parquet (Iceberg)
#     ├── warehouse/lakehouse_db.db/<tabla>/metadata/  metadata JSON, manifest lists y manifests (Avro)
#     └── athena-results/                              resultados de consultas
#   Glue Data Catalog: base lakehouse_db (puntero atómico a la metadata vigente de cada tabla)
#   Athena workgroup "lakehouse" (engine v3, requerido para Iceberg)
#   Política IAM "lakehouse-writer" para el rol de ejecución de Flink
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
data "aws_region" "current" {}

locals {
  account_id     = data.aws_caller_identity.current.account_id
  region         = data.aws_region.current.region
  bucket_name    = "${var.name_prefix}-lakehouse-${local.account_id}"
  warehouse_path = "warehouse"
  db_location    = "s3://${local.bucket_name}/${local.warehouse_path}/${var.database_name}.db"
}

# ===========================================================================
# 1. BUCKET DEL LAKEHOUSE
# ===========================================================================
# Bucket separado del Data Lake de la pre-entrega 1: aquel solo admite objetos vía
# VPC Endpoint, y los actores del Lakehouse (Flink en MSF, Glue, Athena) son
# servicios administrados que acceden desde fuera de la VPC. El acceso se
# gobierna con IAM (rol de Flink, workgroup de Athena) y TLS obligatorio.

resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.bucket_name
  force_destroy = true # entorno dev: permite terraform destroy aunque haya datos y versiones
}

# Versionado: recomendado para Iceberg (recuperación ante borrados accidentales de
# archivos de datos o metadata, además de los snapshots propios de Iceberg).
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
}

# Las versiones viejas solo sirven como red de seguridad: se limpian a los 7 días
# para que los archivos que Iceberg reemplaza (compactación, expiración) no se acumulen.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.lakehouse]
}

# ===========================================================================
# 2. GLUE DATA CATALOG: metastore del Lakehouse
# ===========================================================================

resource "aws_glue_catalog_database" "lakehouse" {
  name         = var.database_name
  description  = "Lakehouse del clickstream: tablas Apache Iceberg escritas por Flink."
  location_uri = local.db_location
}

# ===========================================================================
# 3. ATHENA: consumo SQL de las tablas Iceberg
# ===========================================================================

resource "aws_athena_workgroup" "lakehouse" {
  name          = "${var.name_prefix}-lakehouse"
  description   = "Consultas sobre las tablas Iceberg de ${var.database_name}."
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    engine_version {
      selected_engine_version = "Athena engine version 3" # requerido para Iceberg
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# ===========================================================================
# 4. IAM: permisos del escritor (rol de ejecución de Flink)
# ===========================================================================
# Glue: solo la base lakehouse_db y sus tablas. S3: solo el prefijo warehouse/.

data "aws_iam_policy_document" "writer" {
  statement {
    sid    = "GlueCatalogLakehouseDb"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${var.database_name}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${var.database_name}/*",
    ]
  }

  statement {
    sid       = "ListWarehouse"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.warehouse_path}/*", "${local.warehouse_path}"]
    }
  }

  statement {
    sid    = "ReadWriteWarehouseObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject", # Iceberg borra metadata vieja (write.metadata.delete-after-commit)
      "s3:AbortMultipartUpload",
    ]
    resources = ["${aws_s3_bucket.lakehouse.arn}/${local.warehouse_path}/*"]
  }
}

resource "aws_iam_policy" "writer" {
  name        = "${var.name_prefix}-lakehouse-writer"
  description = "Escritura de tablas Iceberg en ${var.database_name} (Glue Data Catalog + S3 warehouse)."
  policy      = data.aws_iam_policy_document.writer.json
}
