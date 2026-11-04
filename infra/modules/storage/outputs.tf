output "bucket_name" {
  description = "Nombre del bucket del Data Lake."
  value       = aws_s3_bucket.data_lake.bucket
}

output "bucket_arn" {
  description = "ARN del bucket (input del módulo identity)."
  value       = aws_s3_bucket.data_lake.arn
}

output "data_prefix" {
  description = "Prefijo donde el pipeline escribe datos crudos."
  value       = var.data_prefix
}