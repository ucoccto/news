# =========================================================
# Outputs
# =========================================================

output "silver_glue_database_name" {
  description = "Athena/Airflow에서 사용할 Silver Glue Database"
  value       = aws_glue_catalog_database.silver.name
}

output "silver_glue_table_name" {
  description = "Athena/Airflow에서 사용할 Silver Table"
  value       = aws_glue_catalog_table.silver.name
}

output "silver_s3_location" {
  description = "Glue Table이 참조하는 기존 Silver S3 위치"
  value       = "s3://${var.silver_bucket_name}/silver/"
}
