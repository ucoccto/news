# =========================================================
# Variables
# =========================================================

variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "프로젝트명. Glue Database 이름 생성에 사용"
  type        = string
  default     = "de-ai-25-loggen"
}

variable "silver_bucket_name" {
  description = "기존 Silver Parquet 데이터가 실제로 저장되어 있는 S3 Bucket 이름"
  type        = string

  # 예:
  # de-ai-25-loggen-infra-s3-bk-827913617635
  #
  # 이 Terraform은 해당 버킷을 생성/삭제하지 않는다.
  # 반드시 실제 Silver 데이터가 남아 있는 기존 Bucket 이름을 지정한다.
}
