variable "name_prefix" {
  description = "Prefijo para nombrar los recursos (ej: dataplatform-dev)."
  type        = string
}

variable "database_name" {
  description = "Base de datos del Glue Data Catalog que actúa como metastore del Lakehouse."
  type        = string
  default     = "lakehouse_db"
}

variable "table_name" {
  description = "Tabla Iceberg donde escribe el job de Flink (la crea la app si no existe)."
  type        = string
  default     = "user_activity"
}

variable "snapshot_retention_days" {
  description = "Días de snapshots que conserva la tabla (time travel). Se aplica con el mantenimiento (VACUUM)."
  type        = number
  default     = 7
}
