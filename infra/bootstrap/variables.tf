variable "project_name" {
  description = "Prefijo del proyecto. Se usa en el nombre del bucket y de la tabla."
  type        = string
  default     = "dataplatform"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,20}$", var.project_name))
    error_message = "project_name: 3-21 caracteres, minúsculas, números o guiones."
  }
}

variable "region" {
  description = "Región de AWS donde vive el backend."
  type        = string
  default     = "us-east-1"
}