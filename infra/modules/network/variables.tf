variable "name_prefix" {
  description = "Prefijo para nombrar los recursos (ej: dataplatform-dev)."
  type        = string
}

variable "vpc_cidr" {
  description = "Rango de IPs de la VPC."
  type        = string

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "vpc_cidr debe ser un bloque CIDR válido."
  }
}

variable "az_count" {
  description = "Cantidad de zonas de disponibilidad (una subred privada por zona)."
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 4
    error_message = "az_count debe estar entre 2 y 4 (la letra exige al menos 2 AZs)."
  }
}