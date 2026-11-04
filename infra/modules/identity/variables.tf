variable "name_prefix" {
  description = "Prefijo para nombrar roles y políticas (ej: dataplatform-dev)."
  type        = string
}

variable "data_bucket_arn" {
  description = "ARN del bucket del Data Lake al que accede el rol de ejecución."
  type        = string
}

variable "data_prefix" {
  description = "Prefijo del bucket habilitado para el rol de ejecución (ej: raw/)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9_-]+/$", var.data_prefix))
    error_message = "data_prefix debe terminar en '/' (ej: raw/)."
  }
}

variable "audited_bucket_arns" {
  description = "ARNs de los buckets cuya configuración puede leer el rol de auditoría."
  type        = list(string)
}

variable "audited_table_arns" {
  description = "ARNs de las tablas DynamoDB que puede describir el rol de auditoría."
  type        = list(string)
}