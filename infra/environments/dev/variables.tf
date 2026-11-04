variable "project_name" {
  description = "Prefijo del proyecto para nombrar recursos."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,20}$", var.project_name))
    error_message = "project_name: 3-21 caracteres, minúsculas, números o guiones."
  }
}

variable "environment" {
  description = "Nombre del entorno."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment debe ser dev, staging o prod."
  }
}

variable "region" {
  description = "Región de AWS donde se despliega el entorno."
  type        = string
}

variable "vpc_cidr" {
  description = "Rango de IPs de la VPC de datos (ej: 10.20.0.0/16)."
  type        = string

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "vpc_cidr debe ser un bloque CIDR válido."
  }
}