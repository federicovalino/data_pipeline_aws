data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  account_id  = data.aws_caller_identity.current.account_id

  # El nombre del rol de Firehose se define acá (y no dentro del módulo kinesis)
  # para que storage pueda exceptuarlo en su bucket policy sin crear una
  # dependencia circular storage <-> kinesis.
  firehose_role_name = "${local.name_prefix}-firehose-role"
  firehose_role_arn  = "arn:aws:iam::${local.account_id}:role/${local.firehose_role_name}"

  # Recursos del backend, armados con la convención de nombres del bootstrap
  state_bucket_arn = "arn:aws:s3:::${var.project_name}-tfstate-${local.account_id}"
  lock_table_arn   = "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${var.project_name}-tf-locks"
}

module "network" {
  source = "../../modules/network"

  name_prefix = local.name_prefix
  vpc_cidr    = var.vpc_cidr
  az_count    = 2
}

module "storage" {
  source = "../../modules/storage"

  name_prefix     = local.name_prefix
  data_prefix     = "raw/"
  allowed_vpce_id = module.network.s3_endpoint_id # ← output de network como input de storage

  # Firehose escribe desde fuera de la VPC: excepción explícita y acotada a su rol
  vpce_exempt_principal_arns = [local.firehose_role_arn]
}

module "identity" {
  source = "../../modules/identity"

  name_prefix     = local.name_prefix
  data_bucket_arn = module.storage.bucket_arn  # ← output de storage
  data_prefix     = module.storage.data_prefix # ← output de storage

  audited_bucket_arns = [module.storage.bucket_arn, local.state_bucket_arn]
  audited_table_arns  = [local.lock_table_arn]
}
module "kinesis" {
  source = "../../modules/kinesis"

  name_prefix               = local.name_prefix
  expected_write_mb_per_sec = 2 # throughput hipotético → 2 shards
  data_lake_bucket_arn      = module.storage.bucket_arn # ← output de storage
  bronze_prefix             = module.storage.data_prefix
  firehose_role_name        = local.firehose_role_name

  # Buffer agresivo para dev: feedback rápido en S3
  buffer_size_mb          = 5
  buffer_interval_seconds = 60
}
