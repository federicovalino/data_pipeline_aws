# Dónde vive el state de este entorno.
# Los valores concretos vienen de backend.hcl (configuración parcial).
terraform {
  backend "s3" {}
}