# ---------------------------------------------------------------------------
# Módulo network: VPC de datos 100% privada + S3 Gateway Endpoint.
# No define provider ni backend: los hereda del entorno que lo invoca.
# ---------------------------------------------------------------------------

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_region" "current" {}

# --- VPC ----------------------------------------------------------------------
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true # necesario para resolver los endpoints de AWS
  enable_dns_hostnames = true

  tags = { Name = "${var.name_prefix}-vpc" }
}

# --- Subredes privadas: una por AZ ---------------------------------------------
resource "aws_subnet" "private" {
  count = var.az_count

  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index + 10) # 10.20.10.0/24, 10.20.11.0/24...
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false # nada de IPs públicas

  tags = {
    Name = "${var.name_prefix}-private-${data.aws_availability_zones.available.names[count.index]}"
    Tier = "private"
  }
}

# --- Tabla de rutas privada ----------------------------------------------------
# Sin ruta a Internet Gateway ni NAT: solo la ruta local de la VPC
# y la ruta a S3 que agrega el Gateway Endpoint.
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name_prefix}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count = var.az_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# --- S3 Gateway Endpoint -------------------------------------------------------
# Tráfico a S3 por la red interna de AWS: sin NAT, sin costo, sin internet.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id] # asociación con las subredes privadas

  tags = { Name = "${var.name_prefix}-s3-gateway-endpoint" }
}

# --- Security group por defecto sin reglas ------------------------------------
# AWS crea uno por VPC que permite todo el tráfico saliente.
# Al gestionarlo sin reglas, nada que se lance "por defecto" queda expuesto.
resource "aws_default_security_group" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name_prefix}-default-sg-locked" }
}