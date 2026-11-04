output "state_bucket_name" {
  description = "Bucket a usar en el bloque backend de cada entorno."
  value       = aws_s3_bucket.state.bucket
}

output "lock_table_name" {
  description = "Tabla DynamoDB a usar en el bloque backend de cada entorno."
  value       = aws_dynamodb_table.locks.name
}