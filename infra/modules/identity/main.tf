# ---------------------------------------------------------------------------
# Módulo identity: rol de ejecución (data plane) y rol de auditoría
# (control plane, solo lectura). Mínimo privilegio, sin comodines en escritura.
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
}

# ===========================================================================
# 1. ROL DE EJECUCIÓN (Lambda de ingesta)
# ===========================================================================

# Trust policy: solo el servicio Lambda, y solo en nombre de ESTA cuenta
# (aws:SourceAccount previene el ataque de "confused deputy").
data "aws_iam_policy_document" "exec_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "exec" {
  name                 = "${var.name_prefix}-ingest-exec"
  description          = "Rol de ejecución del proceso de ingesta (Lambda)."
  assume_role_policy   = data.aws_iam_policy_document.exec_trust.json
  max_session_duration = 3600
}

# Permisos: EXACTAMENTE los tres que pide la letra, sobre un prefijo.
data "aws_iam_policy_document" "exec_permissions" {
  # ListBucket actúa sobre el BUCKET: el prefijo va en una condición
  statement {
    sid       = "ListOnlyDataPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.data_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [var.data_prefix, "${var.data_prefix}*"]
    }
  }

  # Get/Put actúan sobre OBJETOS: el prefijo va en el ARN
  statement {
    sid       = "ReadWriteOnlyDataPrefix"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${var.data_bucket_arn}/${var.data_prefix}*"]
  }
}

resource "aws_iam_policy" "exec" {
  name        = "${var.name_prefix}-ingest-exec-s3"
  description = "Lectura/escritura sobre un único prefijo del Data Lake."
  policy      = data.aws_iam_policy_document.exec_permissions.json
}

resource "aws_iam_role_policy_attachment" "exec" {
  role       = aws_iam_role.exec.name
  policy_arn = aws_iam_policy.exec.arn
}

# ===========================================================================
# 2. ROL DE AUDITORÍA (plano de control, solo lectura)
# ===========================================================================

# Trust policy: identidades de esta cuenta, y solo con MFA activo.
data "aws_iam_policy_document" "audit_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }

    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }
}

resource "aws_iam_role" "audit" {
  name                 = "${var.name_prefix}-auditor"
  description          = "Rol de auditoría: lectura de configuración, sin acceso a datos."
  assume_role_policy   = data.aws_iam_policy_document.audit_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "audit_permissions" {
  # Red: las acciones ec2:Describe* NO admiten ARN específico (limitación de AWS).
  # Son de solo lectura y se acotan a la región con aws:RequestedRegion.
  statement {
    sid    = "ReadNetworkConfig"
    effect = "Allow"
    actions = [
      "ec2:DescribeVpcs",
      "ec2:DescribeSubnets",
      "ec2:DescribeRouteTables",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeSecurityGroups",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [local.region]
    }
  }

  # Buckets: solo CONFIGURACIÓN (cifrado, versionado, bloqueo público, política).
  # Sin GetObject ni ListBucket: auditar no es leer los datos.
  statement {
    sid    = "ReadBucketConfig"
    effect = "Allow"
    actions = [
      "s3:GetBucketPolicy",
      "s3:GetBucketVersioning",
      "s3:GetEncryptionConfiguration",
      "s3:GetBucketPublicAccessBlock",
      "s3:GetBucketTagging",
      "s3:GetBucketLocation",
    ]
    resources = var.audited_bucket_arns
  }

  # Tabla de locks del backend
  statement {
    sid       = "ReadLockTableConfig"
    effect    = "Allow"
    actions   = ["dynamodb:DescribeTable"]
    resources = var.audited_table_arns
  }

  # IAM: solo los roles y políticas de este proyecto/entorno
  statement {
    sid    = "ReadProjectIam"
    effect = "Allow"
    actions = [
      "iam:GetRole",
      "iam:ListAttachedRolePolicies",
      "iam:ListRolePolicies",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:ListPolicyVersions",
    ]
    resources = [
      "arn:aws:iam::${local.account_id}:role/${var.name_prefix}-*",
      "arn:aws:iam::${local.account_id}:policy/${var.name_prefix}-*",
    ]
  }
}

resource "aws_iam_policy" "audit" {
  name        = "${var.name_prefix}-auditor-readonly"
  description = "Lectura de configuración de red, almacenamiento e IAM del entorno."
  policy      = data.aws_iam_policy_document.audit_permissions.json
}

resource "aws_iam_role_policy_attachment" "audit" {
  role       = aws_iam_role.audit.name
  policy_arn = aws_iam_policy.audit.arn
}