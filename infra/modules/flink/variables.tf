variable "name_prefix" {
  description = "Prefijo para nombrar los recursos (ej: dataplatform-dev)."
  type        = string
}

variable "stream_name" {
  description = "Nombre del Kinesis Data Stream de entrada (output del módulo kinesis)."
  type        = string
}

variable "stream_arn" {
  description = "ARN del Kinesis Data Stream de entrada (output del módulo kinesis)."
  type        = string
}

variable "stream_kms_key_arn" {
  description = "ARN de la llave KMS del stream: Flink necesita kms:Decrypt para leerlo."
  type        = string
}

variable "jar_path" {
  description = "Ruta local al uber-jar de la aplicación (mvn package en flink-app/)."
  type        = string
}

variable "runtime_environment" {
  description = "Versión de Flink del runtime de MSF. Debe coincidir con flink.version del pom.xml."
  type        = string
  default     = "FLINK-1_19"
}

# --- Capacidad --------------------------------------------------------------
variable "parallelism" {
  description = "Paralelismo total del job. Coherente con los shards del stream (1 subtask de source por shard)."
  type        = number
  default     = 2
}

variable "parallelism_per_kpu" {
  description = "Subtasks por KPU. 1 = cada subtask tiene la KPU completa (1 vCPU + 4 GB) para su estado."
  type        = number
  default     = 1
}

# --- Checkpointing ------------------------------------------------------------
variable "checkpoint_interval_ms" {
  description = "Intervalo entre checkpoints. 60 s: equilibrio entre recuperación rápida e I/O hacia S3."
  type        = number
  default     = 60000
}

variable "min_pause_between_checkpoints_ms" {
  description = "Pausa mínima entre el fin de un checkpoint y el inicio del siguiente."
  type        = number
  default     = 5000
}

# --- Lógica temporal (se pasan al código como propiedades de la aplicación) ----
variable "window_minutes" {
  description = "Tamaño de la ventana tumbling, en minutos."
  type        = number
  default     = 1
}

variable "watermark_out_of_orderness_seconds" {
  description = "Tolerancia al desorden del watermark."
  type        = number
  default     = 10
}

variable "watermark_idleness_seconds" {
  description = "Tiempo sin datos tras el cual una subtask de source se marca idle."
  type        = number
  default     = 60
}

variable "allowed_lateness_seconds" {
  description = "Tiempo extra durante el cual una ventana cerrada acepta eventos tardíos y se re-emite."
  type        = number
  default     = 30
}

variable "getrecords_interval_ms" {
  description = <<-EOT
    Intervalo de polling del source de Kinesis. El stream ya tiene otro consumidor (Firehose) y
    el límite de 5 GetRecords/s por shard es compartido: 1000 ms deja a Flink en 1 llamada/s.
  EOT
  type        = number
  default     = 1000
}

variable "start_application" {
  description = "Si es true, Terraform deja la aplicación en estado RUNNING tras el apply."
  type        = bool
  default     = true
}

variable "vpc_config" {
  description = <<-EOT
    Opcional. null = la app usa los endpoints públicos de AWS (Kinesis, CloudWatch, S3),
    suficiente porque fuente y destinos son servicios administrados. Si se indica, MSF crea
    ENIs en estas subredes: como las subredes privadas no tienen NAT, se requieren VPC
    endpoints de interfaz para kinesis-streams, logs y monitoring.
  EOT
  type = object({
    subnet_ids         = list(string)
    security_group_ids = list(string)
  })
  default = null
}

# --- Lakehouse (pre-entrega 5) -------------------------------------------------
variable "iceberg_warehouse" {
  description = "Warehouse de Iceberg (s3://bucket/warehouse). Output del módulo lakehouse."
  type        = string
}

variable "iceberg_database" {
  description = "Base de datos del Glue Data Catalog donde vive la tabla Iceberg."
  type        = string
}

variable "iceberg_table" {
  description = "Tabla Iceberg de salida."
  type        = string
}

variable "additional_policy_arns" {
  description = "Políticas IAM extra para el rol de ejecución (ej. lakehouse-writer)."
  type        = list(string)
  default     = []
}
