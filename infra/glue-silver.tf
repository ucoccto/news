# =========================================================
# Silver Layer - AWS Glue Data Catalog
# =========================================================
#
# [목적]
# 전체 실습 인프라(Kinesis/Flink/Firehose/Lambda 등)를 삭제해도
# 이미 S3에 저장된 Silver Parquet를 Athena/Airflow에서 계속 조회할 수 있도록
# Glue Database와 Silver Table 메타데이터만 별도로 유지한다.
#
# 중요:
# Glue는 실제 데이터를 저장하지 않는다.
# 아래 Table은 기존 S3 Silver 데이터를 가리키는 "메타데이터"이다.
# 따라서 var.silver_bucket_name의 S3 Bucket은 별도로 반드시 남아 있어야 한다.
# =========================================================

# ---------------------------------------------------------
# 1. Silver Glue Database
# ---------------------------------------------------------
resource "aws_glue_catalog_database" "silver" {
  # de-ai-25-loggen
  #       ↓
  # de_ai_25_loggen_silver_glue_db
  name = "${lower(replace(var.project_name, "-", "_"))}_silver_glue_db"

  description = "Silver Parquet 데이터를 Athena/Airflow에서 조회하기 위한 Glue Database"

  # 실습용 메타데이터를 실수로 terraform destroy 하는 것을 방지한다.
  lifecycle {
    prevent_destroy = true
  }
}

# ---------------------------------------------------------
# 2. Silver Glue Table
# ---------------------------------------------------------
resource "aws_glue_catalog_table" "silver" {
  name          = "silver_logs_tbl"
  database_name = aws_glue_catalog_database.silver.name
  table_type    = "EXTERNAL_TABLE"

  # -------------------------------------------------------
  # Partition Projection
  # -------------------------------------------------------
  # 실제 S3 경로:
  #
  # s3://<bucket>/silver/
  #   year=2026/
  #   month=08/
  #   day=26/
  #   hour=14/
  #
  # Glue에 Partition을 매번 등록하지 않고 Athena가
  # year/month/day/hour 값을 이용해 S3 경로를 계산하게 한다.
  # -------------------------------------------------------
  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"

    "projection.enabled" = "true"

    "projection.year.type"  = "integer"
    "projection.year.range" = "2026,2040"

    "projection.month.type"   = "integer"
    "projection.month.range"  = "1,12"
    "projection.month.digits" = "2"

    "projection.day.type"   = "integer"
    "projection.day.range"  = "1,31"
    "projection.day.digits" = "2"

    "projection.hour.type"   = "integer"
    "projection.hour.range"  = "0,23"
    "projection.hour.digits" = "2"

    # 기존 코드에서는 aws_s3_bucket.data.bucket을 직접 참조했지만,
    # Glue-only 구성에서는 S3를 Terraform으로 관리하지 않는다.
    # 따라서 기존 Bucket 이름을 변수로 받아 연결한다.
    "storage.location.template" = "s3://${var.silver_bucket_name}/silver/year=$${year}/month=$${month}/day=$${day}/hour=$${hour}"
  }

  # -------------------------------------------------------
  # Silver Parquet Storage 정보
  # -------------------------------------------------------
  storage_descriptor {
    location = "s3://${var.silver_bucket_name}/silver/"

    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "silver-parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # -----------------------------------------------------
    # Silver 공통 컬럼
    # -----------------------------------------------------
    columns {
      name = "schema_version"
      type = "string"
    }

    columns {
      name = "record_type"
      type = "string"
    }

    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "trace_id"
      type = "string"
    }

    columns {
      name = "run_id"
      type = "string"
    }

    columns {
      name = "occurred_at"
      type = "string"
    }

    columns {
      name = "generated_at_utc"
      type = "string"
    }

    columns {
      name = "domain"
      type = "string"
    }

    columns {
      name = "event_type"
      type = "string"
    }

    # -----------------------------------------------------
    # Silver 중첩 Struct
    # -----------------------------------------------------
    columns {
      name = "service"
      type = "struct<name:string,environment:string,instance_id:string>"
    }

    columns {
      name = "client"
      type = "struct<ip:string,user_agent:string,device_id:string>"
    }

    columns {
      name = "request"
      type = "struct<method:string,path:string,request_bytes:bigint>"
    }

    columns {
      name = "response"
      type = "struct<status_code:int,latency_ms:bigint,response_bytes:bigint>"
    }

    # ecommerce / finance / gaming / smartfactory에서 사용하는
    # 도메인별 필드를 하나의 Superset Struct로 정의한다.
    columns {
      name = "data"
      type = "struct<user_id:string,session_id:string,product_id:string,category:string,quantity:bigint,unit_price:bigint,currency:string,campaign:string,keyword:string,result_count:bigint,order_id:string,total_amount:bigint,payment_method:string,payment_result:string,transaction_id:string,customer_id:string,account_id:string,channel:string,risk_score:double,amount:bigint,merchant_id:string,merchant_category:string,authorization_result:string,destination_bank:string,destination_account_token:string,transfer_result:string,balance:bigint,auth_method:string,login_result:string,player_id:string,server_region:string,player_level:bigint,ping_ms:bigint,platform:string,match_id:string,mode:string,party_size:bigint,result:string,score:bigint,duration_seconds:bigint,item_id:string,currency_type:string,purchase_result:string,quest_id:string,reward_xp:bigint,reward_gold:bigint,plant_id:string,line_id:string,equipment_id:string,equipment_type:string,message_id:string,temperature_c:double,vibration_mm_s:double,pressure_bar:double,rpm:bigint,state:string,runtime_seconds:bigint,lot_id:string,sample_size:bigint,defect_count:bigint,quality_result:string,alarm_code:string,severity:string,acknowledged:boolean,maintenance_type:string,technician_id:string,downtime_minutes:bigint>"
    }

    columns {
      name = "_silver"
      type = "struct<layer:string,processor:string,schema_version:string,processed_at:string>"
    }
  }

  # -------------------------------------------------------
  # Athena에서 WHERE year/month/day/hour 조건에 사용하는
  # Partition Key
  # -------------------------------------------------------
  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  partition_keys {
    name = "day"
    type = "string"
  }

  partition_keys {
    name = "hour"
    type = "string"
  }

  # Database와 마찬가지로 실수로 삭제하지 않도록 보호한다.
  lifecycle {
    prevent_destroy = true
  }
}
