variable "name_prefix" {
  description = "Prefijo para nombrar los recursos (ej: dataplatform-dev)."
  type        = string
}

variable "data_prefix" {
  description = "Prefijo del bucket donde el pipeline escribe datos crudos."
  type        = string
  default     = "raw/"

  validation {
    condition     = can(regex("^[a-z0-9_-]+/$", var.data_prefix))
    error_message = "data_prefix debe terminar en '/' y no empezar con '/' (ej: raw/)."
  }
}

variable "allowed_vpce_id" {
  description = "Si se indica, la lectura/escritura de objetos solo se permite a través de este VPC Endpoint."
  type        = string
  default     = null
}
variable "vpce_exempt_principal_arns" {
  description = "ARNs de roles de servicios administrados (ej. Firehose) que pueden escribir objetos desde fuera de la VPC."
  type        = list(string)
  default     = []
}
