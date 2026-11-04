output "bucket_name" {
  description = "Bucket del Lakehouse."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "warehouse_path" {
  description = "Warehouse de Iceberg (raíz de las tablas del catálogo)."
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/${local.warehouse_path}"
}

output "database_name" {
  description = "Base de datos del Glue Data Catalog."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "table_name" {
  description = "Tabla Iceberg que escribe el job de Flink."
  value       = var.table_name
}

output "athena_workgroup" {
  description = "Workgroup de Athena para consultar el Lakehouse."
  value       = aws_athena_workgroup.lakehouse.name
}

output "writer_policy_arn" {
  description = "Política IAM para escribir en el Lakehouse (se adjunta al rol de Flink)."
  value       = aws_iam_policy.writer.arn
}
