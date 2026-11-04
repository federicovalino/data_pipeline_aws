# ---------------------------------------------------------------------------
# Módulo kinesis: ingesta real-time.
#   Kinesis Data Stream (backbone, PROVISIONED, cifrado KMS)
#     └─► Kinesis Data Firehose ─► S3 Data Lake (capa Bronze, prefijos por fecha)
#   + rol IAM de Firehose, logs de entrega y alarmas de throughput.
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
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region

  # Diseño de shards: cada shard admite 1 MB/s de escritura.
  # 2 MB/s / 1 MB/s = 2 shards. Mínimo 2 para tener paralelismo real.
  shard_write_mb_per_sec = 1
  shard_count            = max(2, ceil(var.expected_write_mb_per_sec / local.shard_write_mb_per_sec))

  stream_name   = "${var.name_prefix}-stream"
  firehose_name = "${var.name_prefix}-firehose"

  # Prefijos dinámicos (Hive style) resueltos por Firehose con la hora de llegada.
  firehose_prefix       = "${var.bronze_prefix}ingesta/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
  firehose_error_prefix = "${var.bronze_prefix}ingesta-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
}

# ===========================================================================
# 1. LLAVE KMS del stream (customer managed: control y auditoría del uso)
# ===========================================================================

resource "aws_kms_key" "kinesis" {
  description             = "Cifrado en reposo del stream ${local.stream_name}"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "kinesis" {
  name          = "alias/${var.name_prefix}-kinesis"
  target_key_id = aws_kms_key.kinesis.key_id
}

# ===========================================================================
# 2. KINESIS DATA STREAM
# ===========================================================================

resource "aws_kinesis_stream" "this" {
  name             = local.stream_name
  shard_count      = local.shard_count
  retention_period = var.retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.kinesis.arn

  # Métricas por shard: permiten ver la distribución de carga y detectar hot shards.
  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds",
  ]

  tags = {
    Name      = local.stream_name
    Component = "ingestion"
    Layer     = "bronze"
  }
}

# ===========================================================================
# 3. IAM: rol de Firehose (leer stream + escribir S3 + logs)
# ===========================================================================

data "aws_iam_policy_document" "firehose_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = var.firehose_role_name
  description        = "Rol de Firehose: lee ${local.stream_name} y entrega en la capa Bronze del Data Lake."
  assume_role_policy = data.aws_iam_policy_document.firehose_trust.json
}

data "aws_iam_policy_document" "firehose_permissions" {
  # Leer del Kinesis Stream (solo este stream)
  statement {
    sid    = "ReadSourceStream"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.this.arn]
  }

  # El stream está cifrado: para leerlo Firehose necesita descifrar con la llave,
  # y solo a través del servicio Kinesis.
  statement {
    sid       = "DecryptStreamRecords"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.kinesis.arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${local.region}.amazonaws.com"]
    }
  }

  # Acciones a nivel de bucket (no admiten prefijo en el ARN)
  statement {
    sid    = "BucketLevel"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [var.data_lake_bucket_arn]
  }

  # Escribir objetos solo debajo de la capa Bronze
  statement {
    sid    = "WriteBronzeObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["${var.data_lake_bucket_arn}/${var.bronze_prefix}*"]
  }

  # Escribir logs de entrega en CloudWatch (solo su log group)
  statement {
    sid       = "WriteDeliveryLogs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.firehose_role_name}-policy"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}

# ===========================================================================
# 4. LOGS de entrega de Firehose
# ===========================================================================

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.firehose_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "firehose_s3" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# ===========================================================================
# 5. KINESIS DATA FIREHOSE: stream ─► S3 (Bronze)
# ===========================================================================

resource "aws_kinesis_firehose_delivery_stream" "this" {
  name = local.firehose_name

  # En el provider AWS v5+ el destino S3 se declara como "extended_s3"
  # (el valor "s3" fue eliminado).
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.this.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.data_lake_bucket_arn

    prefix              = local.firehose_prefix
    error_output_prefix = local.firehose_error_prefix

    # Buffer agresivo para desarrollo: se entrega a los 5 MB o a los 60 s,
    # lo que ocurra primero.
    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_seconds

    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3.name
    }
  }

  tags = {
    Name      = local.firehose_name
    Component = "ingestion"
    Layer     = "bronze"
  }

  # El rol tiene que tener sus permisos antes de que Firehose empiece a leer.
  depends_on = [aws_iam_role_policy.firehose]
}

# ===========================================================================
# 6. OBSERVABILIDAD: alarmas de throughput excedido
# ===========================================================================

resource "aws_cloudwatch_metric_alarm" "write_throughput_exceeded" {
  alarm_name          = "${local.stream_name}-write-throughput-exceeded"
  alarm_description   = "Productores rechazados por superar 1 MB/s o 1.000 registros/s en algún shard (posible hot shard o falta de shards)."
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  dimensions          = { StreamName = aws_kinesis_stream.this.name }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "read_throughput_exceeded" {
  alarm_name          = "${local.stream_name}-read-throughput-exceeded"
  alarm_description   = "Consumidores superando 2 MB/s o 5 lecturas/s por shard (consumidores compitiendo; evaluar Enhanced Fan-Out)."
  namespace           = "AWS/Kinesis"
  metric_name         = "ReadProvisionedThroughputExceeded"
  dimensions          = { StreamName = aws_kinesis_stream.this.name }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions
}
