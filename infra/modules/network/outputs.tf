output "vpc_id" {
  description = "ID de la VPC de datos."
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "CIDR de la VPC."
  value       = aws_vpc.this.cidr_block
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas (para Lambda / Flink en próximas entregas)."
  value       = aws_subnet.private[*].id
}

output "private_route_table_id" {
  description = "ID de la tabla de rutas privada."
  value       = aws_route_table.private.id
}

output "s3_endpoint_id" {
  description = "ID del Gateway Endpoint de S3 (para bucket policies con aws:SourceVpce)."
  value       = aws_vpc_endpoint.s3.id
}

output "s3_endpoint_prefix_list_id" {
  description = "Prefix list de S3 (para reglas de security groups hacia S3)."
  value       = aws_vpc_endpoint.s3.prefix_list_id
}