Glue-only Terraform
===================

목적
----
전체 실습 인프라를 제거한 뒤에도 기존 Silver Parquet를 Athena/Airflow에서
조회할 수 있도록 Glue Database와 silver_logs_tbl만 별도 Terraform state로 관리합니다.

중요
----
Glue는 실제 데이터를 보관하지 않습니다.
Silver Parquet가 저장된 S3 Bucket은 반드시 별도로 남아 있어야 합니다.
이 구성은 S3 Bucket을 생성하거나 삭제하지 않고 이름만 참조합니다.

구성
----
version.tf
provider.tf
variables.tf
glue-silver.tf
outputs.tf
terraform.tfvars.example

신규 생성이라면
------------
1. terraform.tfvars.example을 terraform.tfvars로 복사
2. silver_bucket_name을 실제 Bucket으로 확인
3. terraform init
4. terraform plan
5. terraform apply

기존 Glue Database/Table이 이미 AWS에 있다면
--------------------------------------------
새 Terraform에서 바로 apply 하면 AlreadyExists 오류가 날 수 있습니다.
기존 리소스를 새 state로 옮겨야 합니다.

[기존 infra 디렉터리]
먼저 기존 Glue 리소스 주소를 확인:
  terraform state list

기존 state에서 AWS 리소스를 삭제하지 않고 연결만 해제:
  terraform state rm aws_glue_catalog_table.silver
  terraform state rm aws_glue_catalog_database.silver

그리고 기존 infra의 glue-silver.tf는 제거하거나 이 별도 디렉터리로 이동합니다.

[glue-only-infra 디렉터리]
terraform.tfvars 준비 후:
  terraform init

AWS Account ID가 827913617635이고 Database 이름이
  de_ai_25_loggen_silver_glue_db
이라면:

  terraform import aws_glue_catalog_database.silver 827913617635:de_ai_25_loggen_silver_glue_db

  terraform import aws_glue_catalog_table.silver 827913617635:de_ai_25_loggen_silver_glue_db:silver_logs_tbl

마지막으로:
  terraform plan

plan 결과가 현재 Glue 설정과 동일하면 생성/삭제 없이 관리가 이전됩니다.

주의
----
이 파일에는 lifecycle.prevent_destroy=true가 들어 있습니다.
따라서 glue-only-infra에서 terraform destroy를 실행하면 Glue Database/Table 삭제가 차단됩니다.
