output "exec_role_arn" {
  description = "ARN del rol de ejecución (lo usará la Lambda en la próxima pre-entrega)."
  value       = aws_iam_role.exec.arn
}

output "exec_role_name" {
  description = "Nombre del rol de ejecución (para adjuntarle políticas futuras)."
  value       = aws_iam_role.exec.name
}

output "audit_role_arn" {
  description = "ARN del rol de auditoría."
  value       = aws_iam_role.audit.arn
}