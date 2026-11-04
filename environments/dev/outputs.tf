output "vpc_id" {
  description = "ID de la VPC de datos."
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas."
  value       = module.network.private_subnet_ids
}

output "s3_endpoint_id" {
  description = "ID del S3 Gateway Endpoint."
  value       = module.network.s3_endpoint_id
}

output "exec_role_arn" {
  description = "ARN del rol de ejecución."
  value       = module.identity.exec_role_arn
}

output "audit_role_arn" {
  description = "ARN del rol de auditoría."
  value       = module.identity.audit_role_arn
}

output "data_lake_bucket_name" {
  description = "Bucket del Data Lake."
  value       = module.storage.bucket_name
}
output "kinesis_stream_name" {
  description = "Nombre del Kinesis Data Stream."
  value       = module.kinesis.stream_name
}

output "kinesis_shard_count" {
  description = "Cantidad de shards del stream."
  value       = module.kinesis.shard_count
}

output "firehose_name" {
  description = "Nombre del delivery stream de Firehose."
  value       = module.kinesis.firehose_name
}

output "bronze_s3_prefix" {
  description = "Prefijo de S3 donde Firehose entrega los datos crudos."
  value       = module.kinesis.bronze_s3_prefix
}

output "flink_application_name" {
  description = "Aplicación de Managed Service for Apache Flink."
  value       = module.flink.application_name
}

output "flink_log_group" {
  description = "Log group con USER_ACTIVITY, CART_ABANDONMENT_ALERT y LATE_EVENT."
  value       = module.flink.log_group_name
}

output "flink_kpus" {
  description = "KPUs de procesamiento del job (+1 de orquestación)."
  value       = module.flink.kpus
}
