# ---------------------------------------------------------------------------
# Módulo redshift: capa de analítica in-stream (pre-entrega 6).
#
#   Kinesis Data Stream ──(Streaming Ingestion, IAM)──► Redshift Serverless
#   Glue Data Catalog / S3 (Iceberg) ──(external schema, IAM)──► Redshift Serverless
#
#   - Namespace + workgroup Serverless en las subredes PRIVADAS de la pre-entrega 1
#   - Rol IAM de servicio con mínimo privilegio: leer UN stream, descifrar con SU
#     clave KMS, leer metadatos de UNA base de Glue y solo el prefijo warehouse/
#   - Interface VPC Endpoint de Kinesis: la ingesta no sale a Internet (la VPC no
#     tiene NAT ni Internet Gateway)
#   - Límite de uso diario de RPU-horas: corta el gasto si algo queda prendido
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
  account_id    = data.aws_caller_identity.current.account_id
  region        = data.aws_region.current.region
  lakehouse_arn = "arn:aws:s3:::${var.lakehouse_bucket_name}"
}

# ===========================================================================
# 1. ROL IAM DE SERVICIO (Redshift → Kinesis, Glue, S3)
# ===========================================================================
# Trust policy: solo los servicios de Redshift pueden asumirlo. Serverless usa
# redshift-serverless.amazonaws.com; se incluye redshift.amazonaws.com porque
# el namespace también lo presenta al asumir roles para Spectrum/streaming.
data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "RedshiftAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift" {
  name                 = "${var.name_prefix}-redshift-streaming-role"
  description          = "Redshift Serverless: Streaming Ingestion desde Kinesis y lectura de Iceberg en Glue/S3"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "redshift" {
  # --- Streaming Ingestion: solo lectura, solo este stream, sin comodines en acciones
  statement {
    sid    = "ReadClickstream"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [var.stream_arn]
  }

  # ListStreams no admite restricción por recurso (requisito de RSI)
  statement {
    sid       = "ListStreams"
    effect    = "Allow"
    actions   = ["kinesis:ListStreams"]
    resources = ["*"]
  }

  # El stream está cifrado con una CMK (pre-entrega 2): sin Decrypt no hay lectura
  statement {
    sid       = "DecryptStream"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [var.stream_kms_key_arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${local.region}.amazonaws.com"]
    }
  }

  # --- Lakehouse: lectura de metadatos de UNA base del Glue Data Catalog
  statement {
    sid    = "ReadLakehouseCatalog"
    effect = "Allow"
    actions = [
      "glue:GetCatalog",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${var.glue_database_name}/*",
    ]
  }

  # --- Lakehouse: solo LECTURA del prefijo warehouse/ (Parquet + metadata Iceberg)
  statement {
    sid       = "ListWarehouse"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.lakehouse_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [var.warehouse_prefix, "${var.warehouse_prefix}/*"]
    }
  }

  statement {
    sid       = "BucketLocation"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = [local.lakehouse_arn]
  }

  statement {
    sid       = "ReadWarehouseObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${local.lakehouse_arn}/${var.warehouse_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "redshift" {
  name   = "streaming-ingestion-and-lakehouse-read"
  role   = aws_iam_role.redshift.id
  policy = data.aws_iam_policy_document.redshift.json
}

# ===========================================================================
# 2. RED: security groups + Interface VPC Endpoint de Kinesis
# ===========================================================================
# La VPC de la pre-entrega 1 es 100% privada (sin NAT ni IGW). Para que Redshift
# llegue a Kinesis sin pasar por Internet se usa PrivateLink.
resource "aws_security_group" "redshift" {
  name        = "${var.name_prefix}-redshift-sg"
  description = "Redshift Serverless: sin ingreso desde Internet"
  vpc_id      = var.vpc_id

  tags = { Name = "${var.name_prefix}-redshift-sg" }
}

# Clientes SQL solo desde dentro de la VPC (Query Editor v2 no necesita regla)
resource "aws_vpc_security_group_ingress_rule" "redshift_sql" {
  security_group_id = aws_security_group.redshift.id
  description       = "SQL (5439) solo desde la VPC"
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 5439
  to_port           = 5439
}

# Salida HTTPS solo hacia la VPC (endpoint de Kinesis)
resource "aws_vpc_security_group_egress_rule" "redshift_https_vpc" {
  security_group_id = aws_security_group.redshift.id
  description       = "HTTPS hacia los VPC endpoints"
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_security_group" "kinesis_endpoint" {
  name        = "${var.name_prefix}-kinesis-endpoint-sg"
  description = "Interface endpoint de Kinesis: HTTPS solo desde Redshift"
  vpc_id      = var.vpc_id

  tags = { Name = "${var.name_prefix}-kinesis-endpoint-sg" }
}

resource "aws_vpc_security_group_ingress_rule" "kinesis_endpoint_from_redshift" {
  security_group_id            = aws_security_group.kinesis_endpoint.id
  description                  = "HTTPS desde el workgroup de Redshift"
  referenced_security_group_id = aws_security_group.redshift.id
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${local.region}.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.subnet_ids
  security_group_ids  = [aws_security_group.kinesis_endpoint.id]
  private_dns_enabled = true # kinesis.<region>.amazonaws.com resuelve a IPs privadas

  tags = { Name = "${var.name_prefix}-kinesis-interface-endpoint" }
}

# ===========================================================================
# 3. REDSHIFT SERVERLESS
# ===========================================================================
resource "aws_redshiftserverless_namespace" "this" {
  namespace_name = "${var.name_prefix}-analytics"
  db_name        = var.database_name
  admin_username = var.admin_username

  # La contraseña la genera y guarda Redshift en Secrets Manager:
  # no aparece en el código ni en el tfstate.
  manage_admin_password = true

  iam_roles            = [aws_iam_role.redshift.arn]
  default_iam_role_arn = aws_iam_role.redshift.arn

  log_exports = ["userlog", "connectionlog", "useractivitylog"]
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = "${var.name_prefix}-analytics"
  base_capacity  = var.base_capacity_rpu

  subnet_ids           = var.subnet_ids # 2 subredes en 2 AZ (mínimo sin Enhanced VPC Routing)
  security_group_ids   = [aws_security_group.redshift.id]
  publicly_accessible  = false
  enhanced_vpc_routing = false

  # TLS obligatorio para cualquier cliente SQL
  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }

  depends_on = [aws_vpc_endpoint.kinesis]
}

# Tope de gasto: al superar N RPU-horas en el día, el workgroup se desactiva
resource "aws_redshiftserverless_usage_limit" "compute" {
  resource_arn  = aws_redshiftserverless_workgroup.this.arn
  usage_type    = "serverless-compute"
  amount        = var.daily_rpu_hours_limit
  period        = "daily"
  breach_action = "deactivate"
}
