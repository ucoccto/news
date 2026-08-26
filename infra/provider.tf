# =========================================================
# AWS Provider
# =========================================================
# 이 Terraform은 Silver 실습을 계속하기 위해
# Glue Data Catalog의 Database/Table만 별도로 관리한다.
#
# S3, Kinesis, Flink, Firehose, Lambda 등의 리소스는
# 이 Terraform에서 생성하거나 삭제하지 않는다.

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project  = var.project_name
      ManageBy = "Terraform"
      Purpose  = "Silver Glue Catalog persistence"
    }
  }
}
