variable "name_prefix" {
  description = "Prefijo para nombrar recursos (<proyecto>-<entorno>)."
  type        = string
}

variable "vpc_id" {
  description = "VPC privada de la pre-entrega 1."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR de la VPC (reglas de security group)."
  type        = string
}

variable "subnet_ids" {
  description = "Subredes privadas para el workgroup y el endpoint de Kinesis (mínimo 2, en 2 AZ)."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) >= 2
    error_message = "Redshift Serverless sin Enhanced VPC Routing requiere al menos 2 subredes en 2 AZ."
  }
}

variable "stream_arn" {
  description = "ARN del Kinesis Data Stream que consume la Streaming Ingestion."
  type        = string
}

variable "stream_kms_key_arn" {
  description = "ARN de la CMK con la que está cifrado el stream."
  type        = string
}

variable "lakehouse_bucket_name" {
  description = "Bucket del Lakehouse (pre-entrega 5)."
  type        = string
}

variable "warehouse_prefix" {
  description = "Prefijo del warehouse Iceberg dentro del bucket."
  type        = string
  default     = "warehouse"
}

variable "glue_database_name" {
  description = "Base del Glue Data Catalog con las tablas Iceberg."
  type        = string
}

variable "database_name" {
  description = "Base de datos de Redshift."
  type        = string
  default     = "analytics"
}

variable "admin_username" {
  description = "Usuario administrador del namespace (la contraseña la gestiona Secrets Manager)."
  type        = string
  default     = "rs_admin"
}

variable "base_capacity_rpu" {
  description = "Capacidad base del workgroup en RPU."
  type        = number
  default     = 8

  validation {
    condition     = var.base_capacity_rpu >= 4 && var.base_capacity_rpu <= 1024
    error_message = "base_capacity_rpu debe estar entre 4 y 1024."
  }
}

variable "daily_rpu_hours_limit" {
  description = "Máximo de RPU-horas por día antes de desactivar el workgroup (control de costos)."
  type        = number
  default     = 16
}
