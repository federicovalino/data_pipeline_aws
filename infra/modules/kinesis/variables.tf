variable "name_prefix" {
  description = "Prefijo para nombrar los recursos (ej: dataplatform-dev). Se arma con project_name y environment."
  type        = string
}

variable "expected_write_mb_per_sec" {
  description = "Throughput de escritura esperado en MB/s. Define la cantidad de shards (1 MB/s por shard)."
  type        = number
  default     = 2

  validation {
    condition     = var.expected_write_mb_per_sec > 0
    error_message = "expected_write_mb_per_sec debe ser mayor a 0."
  }
}

variable "retention_hours" {
  description = "Retención del stream en horas (24 a 8760)."
  type        = number
  default     = 24

  validation {
    condition     = var.retention_hours >= 24 && var.retention_hours <= 8760
    error_message = "retention_hours debe estar entre 24 y 8760."
  }
}

variable "data_lake_bucket_arn" {
  description = "ARN del bucket del Data Lake (output del módulo storage)."
  type        = string
}

variable "bronze_prefix" {
  description = "Prefijo raíz de la capa Bronze dentro del bucket. Firehose solo puede escribir debajo de él."
  type        = string
  default     = "raw/"

  validation {
    condition     = can(regex("^[a-z0-9_-]+/$", var.bronze_prefix))
    error_message = "bronze_prefix debe terminar en '/' y no empezar con '/' (ej: raw/)."
  }
}

variable "firehose_role_name" {
  description = "Nombre del rol de Firehose. Se recibe desde el entorno para que storage pueda referenciar su ARN sin dependencias circulares."
  type        = string
}

variable "buffer_size_mb" {
  description = "Buffer de Firehose en MB. En dev se usa un valor bajo para ver archivos rápido."
  type        = number
  default     = 5
}

variable "buffer_interval_seconds" {
  description = "Intervalo máximo de buffer de Firehose en segundos (60 a 900)."
  type        = number
  default     = 60
}

variable "alarm_actions" {
  description = "ARNs a notificar cuando se dispara una alarma (ej: un tópico SNS). Vacío = solo se registra el estado."
  type        = list(string)
  default     = []
}
