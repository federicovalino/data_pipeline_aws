output "stream_name" {
  description = "Nombre del Kinesis Data Stream."
  value       = aws_kinesis_stream.this.name
}

output "stream_arn" {
  description = "ARN del Kinesis Data Stream (input de futuros consumidores)."
  value       = aws_kinesis_stream.this.arn
}

output "shard_count" {
  description = "Cantidad de shards calculada."
  value       = aws_kinesis_stream.this.shard_count
}

output "kms_key_arn" {
  description = "ARN de la llave KMS del stream (los productores necesitan kms:GenerateDataKey sobre ella)."
  value       = aws_kms_key.kinesis.arn
}

output "firehose_name" {
  description = "Nombre del delivery stream de Firehose."
  value       = aws_kinesis_firehose_delivery_stream.this.name
}

output "firehose_role_arn" {
  description = "ARN del rol de Firehose."
  value       = aws_iam_role.firehose.arn
}

output "bronze_s3_prefix" {
  description = "Prefijo de S3 donde Firehose entrega los datos crudos."
  value       = "${var.bronze_prefix}ingesta/"
}
