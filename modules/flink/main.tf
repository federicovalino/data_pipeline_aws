# ---------------------------------------------------------------------------
# Módulo flink: aplicación de Amazon Managed Service for Apache Flink.
#   S3 (artefacto .jar) ─► MSF (job stateful) ◄─ Kinesis Data Stream (pre-entrega 2)
#                               └─► CloudWatch Logs (resultados, alertas, eventos tardíos)
#   Checkpoints y snapshots: S3 administrado por el servicio.
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
  app_name   = "${var.name_prefix}-clickstream-flink"
  jar_key    = "flink/clickstream-processor/${basename(var.jar_path)}"
}

# ===========================================================================
# 1. ARTEFACTO: bucket propio para el código de la aplicación
# ===========================================================================
# Bucket separado del Data Lake: la política del Data Lake solo admite objetos vía
# el VPC Endpoint, y MSF descarga el código desde fuera de la VPC.

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.name_prefix}-flink-artifacts-${local.account_id}"
  force_destroy = true # solo artefactos de build: se pueden regenerar
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled" # MSF referencia una versión concreta del .jar
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.artifacts]
}

# Subida del .jar. source_hash: si cambia el código, cambia la versión del objeto
# y Terraform actualiza la aplicación para que use la nueva.
resource "aws_s3_object" "jar" {
  bucket      = aws_s3_bucket.artifacts.id
  key         = local.jar_key
  source      = var.jar_path
  source_hash = filemd5(var.jar_path)

  depends_on = [aws_s3_bucket_versioning.artifacts]
}

# ===========================================================================
# 2. LOGS: resultados del job, alertas y eventos tardíos
# ===========================================================================

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${local.app_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "kinesis-analytics-log-stream"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# ===========================================================================
# 3. IAM: rol de ejecución de la aplicación (mínimo privilegio)
# ===========================================================================

data "aws_iam_policy_document" "trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${var.name_prefix}-flink-exec-role"
  description        = "Rol de ejecución de ${local.app_name}: lee el stream y escribe logs."
  assume_role_policy = data.aws_iam_policy_document.trust.json
}

data "aws_iam_policy_document" "flink" {
  # Leer el código de la aplicación (solo este objeto)
  statement {
    sid       = "ReadApplicationCode"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.artifacts.arn}/${local.jar_key}"]
  }

  # Leer el Kinesis Data Stream de la pre-entrega 2 (solo ese stream)
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
    resources = [var.stream_arn]
  }

  # El stream está cifrado con una CMK: descifrar solo a través de Kinesis
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

  # Escribir los logs de la aplicación (solo su log group)
  statement {
    sid    = "WriteApplicationLogs"
    effect = "Allow"
    actions = [
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]
    resources = [
      aws_cloudwatch_log_group.flink.arn,
      "${aws_cloudwatch_log_group.flink.arn}:*",
    ]
  }

  # Métricas del conector y de la aplicación (PutMetricData no admite ARN de recurso)
  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AWS/KinesisAnalytics"]
    }
  }

  # Solo si la app corre dentro de la VPC: MSF crea y administra sus ENIs
  dynamic "statement" {
    for_each = var.vpc_config == null ? [] : [1]
    content {
      sid    = "ManageVpcEnis"
      effect = "Allow"
      actions = [
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeDhcpOptions",
        "ec2:DescribeNetworkInterfaces",
        "ec2:CreateNetworkInterface",
        "ec2:CreateNetworkInterfacePermission",
        "ec2:DeleteNetworkInterface",
      ]
      resources = ["*"]
    }
  }
}

resource "aws_iam_role_policy" "flink" {
  name   = "${var.name_prefix}-flink-exec-policy"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}

# ===========================================================================
# 4. APLICACIÓN DE MANAGED SERVICE FOR APACHE FLINK
# ===========================================================================

resource "aws_kinesisanalyticsv2_application" "this" {
  name                   = local.app_name
  description            = "Actividad por usuario en ventanas de 1 minuto y detección de carritos abandonados."
  runtime_environment    = var.runtime_environment
  service_execution_role = aws_iam_role.flink.arn
  start_application      = var.start_application

  application_configuration {

    # Código: el uber-jar subido a S3, en una versión concreta
    application_code_configuration {
      code_content_type = "ZIPFILE" # tipo que usa MSF para un .jar
      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.artifacts.arn
          file_key       = aws_s3_object.jar.key
          object_version = aws_s3_object.jar.version_id
        }
      }
    }

    # Parámetros que lee el código (ClickstreamProcessingJob.loadApplicationProperties)
    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          "stream.name"                        = var.stream_name
          "aws.region"                         = local.region
          "window.minutes"                     = tostring(var.window_minutes)
          "watermark.out.of.orderness.seconds" = tostring(var.watermark_out_of_orderness_seconds)
          "watermark.idleness.seconds"         = tostring(var.watermark_idleness_seconds)
          "allowed.lateness.seconds"           = tostring(var.allowed_lateness_seconds)
          "kinesis.getrecords.interval.ms"     = tostring(var.getrecords_interval_ms)
        }
      }
    }

    flink_application_configuration {
      # Checkpointing: estado RocksDB respaldado en el S3 administrado por MSF.
      # Sin esto, un reinicio o un reescalado perdería las ventanas abiertas
      # y los carritos pendientes de cada usuario.
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = var.min_pause_between_checkpoints_ms
      }

      # Logs INFO (resultados y alertas) y métricas por task (lag, backpressure, checkpoints)
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }

      # Capacidad: KPUs = parallelism / parallelism_per_kpu (+1 KPU de orquestación)
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.parallelism
        parallelism_per_kpu  = var.parallelism_per_kpu
        auto_scaling_enabled = false # escalado manual y predecible (desde snapshot)
      }
    }

    # Snapshots: permiten actualizar código o paralelismo sin perder el estado
    application_snapshot_configuration {
      snapshots_enabled = true
    }

    run_configuration {
      application_restore_configuration {
        application_restore_type = "RESTORE_FROM_LATEST_SNAPSHOT"
      }
      flink_run_configuration {
        allow_non_restored_state = false # si un operador no recupera su estado, falla (no lo descarta)
      }
    }

    # Conectividad: opcional, ver var.vpc_config
    dynamic "vpc_configuration" {
      for_each = var.vpc_config == null ? [] : [var.vpc_config]
      content {
        subnet_ids         = vpc_configuration.value.subnet_ids
        security_group_ids = vpc_configuration.value.security_group_ids
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  tags = {
    Component = "processing"
    Layer     = "realtime"
  }

  # El rol necesita sus permisos antes de que la aplicación arranque
  depends_on = [aws_iam_role_policy.flink]
}
