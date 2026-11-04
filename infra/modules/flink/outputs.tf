output "application_name" {
  description = "Nombre de la aplicación de Managed Service for Apache Flink."
  value       = aws_kinesisanalyticsv2_application.this.name
}

output "application_arn" {
  description = "ARN de la aplicación."
  value       = aws_kinesisanalyticsv2_application.this.arn
}

output "artifacts_bucket" {
  description = "Bucket con el .jar de la aplicación."
  value       = aws_s3_bucket.artifacts.bucket
}

output "log_group_name" {
  description = "Log group de CloudWatch con resultados, alertas y eventos tardíos."
  value       = aws_cloudwatch_log_group.flink.name
}

output "kpus" {
  description = "KPUs de procesamiento (parallelism / parallelism_per_kpu). MSF suma 1 KPU de orquestación."
  value       = ceil(var.parallelism / var.parallelism_per_kpu)
}
