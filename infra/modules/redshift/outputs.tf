output "namespace_name" {
  description = "Namespace de Redshift Serverless."
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "workgroup_name" {
  description = "Workgroup de Redshift Serverless (conectarse desde Query Editor v2)."
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "database_name" {
  description = "Base de datos de Redshift."
  value       = aws_redshiftserverless_namespace.this.db_name
}

output "iam_role_arn" {
  description = "Rol IAM de servicio para CREATE EXTERNAL SCHEMA (Kinesis y Glue)."
  value       = aws_iam_role.redshift.arn
}

output "admin_secret_arn" {
  description = "Secreto de Secrets Manager con la contraseña del administrador."
  value       = aws_redshiftserverless_namespace.this.admin_password_secret_arn
}

output "kinesis_endpoint_id" {
  description = "Interface VPC Endpoint de Kinesis."
  value       = aws_vpc_endpoint.kinesis.id
}
