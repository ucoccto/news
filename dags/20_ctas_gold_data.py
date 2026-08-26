# =========================================================
# DAG 1. Silver -> Gold (CTAS 학습용)
# =========================================================
#
# [의도]
# Athena의 CTAS(Create Table As Select) 개념을 명확하게 학습하기 위한 DAG이다.
#
# 핵심 흐름:
#
#   Silver(silver_logs_tbl)
#           ↓
#   T1. 기존 Gold 테이블 DROP
#           ↓
#   T2. 기존 Gold S3 데이터 삭제
#           ↓
#   T3. CTAS
#       CREATE TABLE ... AS SELECT ...
#           ↓
#   Gold(gold_daily_report_ctas_tbl)
#
# CTAS는 "SELECT 결과를 이용해 새로운 테이블과 실제 데이터를 동시에 생성"한다.
#
# 주의:
# Athena CTAS의 external_location은 비어 있어야 한다.
# 따라서 DAG를 재실행할 경우 DROP TABLE만으로는 부족하고,
# S3의 기존 Gold 파일도 삭제해야 한다.
#
# 이 DAG는 "CTAS 개념 학습"에 초점을 둔 예제이며,
# 매일 과거 데이터를 누적하는 운영형 Gold에는 DAG 2 방식이 더 적합하다.
#
# 수동 테스트 예:
# Airflow Trigger DAG Config
#
# {
#   "target_date": "2026-08-25"
# }
#
# =========================================================

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator


# =========================================================
# 1. 환경 설정
# =========================================================

AWS_CONN_ID = "aws_default"
AWS_REGION = "ap-northeast-2"

# 실제 프로젝트 값과 다르면 여기만 수정
BUCKET_NAME = "de-ai-25-loggen-s3-bk-827913617635"

# silver_logs_tbl이 등록되어 있는 Athena/Glue Database
DATABASE_NAME = "de_ai_25_loggen_silver_glue_db"

SILVER_TABLE = "silver_logs_tbl"

# 운영형 DAG와 충돌하지 않도록 CTAS 전용 테이블명 사용
GOLD_TABLE = "gold_daily_report_ctas_tbl"

# Athena SQL 실행 결과 저장 위치
QUERY_RESULT_S3 = f"s3://{BUCKET_NAME}/athena/dags/results/"

# CTAS가 실제 Parquet 결과를 저장할 위치
GOLD_PREFIX = "gold/daily_report_ctas/"
GOLD_LOCATION = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}"


# =========================================================
# 2. 처리 대상 날짜
# =========================================================

TARGET_DATE = "{{ dag_run.conf.get('target_date', ds) }}"
TARGET_YEAR = "{{ dag_run.conf.get('target_date', ds)[0:4] }}"
TARGET_MONTH = "{{ dag_run.conf.get('target_date', ds)[5:7] }}"
TARGET_DAY = "{{ dag_run.conf.get('target_date', ds)[8:10] }}"

print("날짜", TARGET_DATE)

# =========================================================
# 3. DAG 정의
# =========================================================

with DAG(
    dag_id="20_silver_to_gold_ctas_daily_report",
    description="Silver 로그를 Athena CTAS로 집계하여 Gold Parquet 생성",
    default_args={
        "owner": "de_ai_25",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },

    # 매일 00:00 실행
    schedule="0 0 * * *",

    # 한국 시간 기준
    start_date=pendulum.datetime(
        2026,
        8,
        25,
        tz="Asia/Seoul",
    ),

    catchup=False,

    tags=[
        "aws",
        "airflow",
        "athena",
        "ctas",
        "silver",
        "gold",
    ],
) as dag:

    # =====================================================
    # T1. 기존 CTAS Gold 테이블 삭제
    # =====================================================
    #
    # CTAS는 CREATE TABLE을 수행하므로
    # 동일 이름의 테이블이 존재하면 다시 생성할 수 없다.
    #
    # DROP TABLE은 Glue/Athena 메타데이터만 삭제하고
    # S3의 실제 Parquet 파일은 삭제하지 않는다.
    # =====================================================

    t1_drop_gold_table = AthenaOperator(
        task_id="drop_gold_ctas_table",

        query=f"""
            DROP TABLE IF EXISTS {GOLD_TABLE}
        """,

        database=DATABASE_NAME,
        output_location=QUERY_RESULT_S3,
        aws_conn_id=AWS_CONN_ID,
        
    )


    # =====================================================
    # T2. 기존 Gold S3 데이터 삭제
    # =====================================================
    #
    # CTAS의 external_location은 비어 있어야 한다.
    #
    # DROP TABLE 이후에도 기존 Parquet 파일은 S3에 남으므로
    # 같은 DAG를 다시 실행하려면 실제 S3 데이터도 삭제한다.
    #
    # DROP TABLE
    #   = 메타데이터 삭제
    #
    # S3DeleteObjectsOperator
    #   = 실제 데이터 삭제
    # =====================================================

    t2_delete_gold_s3 = S3DeleteObjectsOperator(
        task_id="delete_gold_ctas_s3_data",

        bucket=BUCKET_NAME,
        prefix=GOLD_PREFIX,

        aws_conn_id=AWS_CONN_ID,
    )


    # =====================================================
    # T3. CTAS 실행
    # =====================================================
    #
    # CTAS = CREATE TABLE AS SELECT
    #
    # 하나의 SQL에서 동시에 수행:
    #
    #   1) Silver 데이터 SELECT
    #   2) GROUP BY / 집계
    #   3) Gold 테이블 생성
    #   4) 결과를 Parquet로 변환
    #   5) S3 external_location에 저장
    #
    # Silver -> Athena SQL -> Gold Table + Gold Parquet
    # =====================================================

    t3_create_gold_with_ctas = AthenaOperator(
        task_id="create_gold_with_ctas",

        query=f"""
            CREATE TABLE {GOLD_TABLE}
            WITH (
                format = 'PARQUET',
                external_location = '{GOLD_LOCATION}'
            ) AS SELECT
                DATE('{TARGET_DATE}') AS report_date,
                domain,
                event_type,
                COALESCE( service.name,'unknown' ) AS service_name,
                COUNT(*) AS total_count,
                COUNT(response.status_code) AS response_count,
                COUNT_IF( response.status_code >= 200 AND response.status_code < 400 ) AS success_count,
                COUNT_IF( response.status_code >= 400 ) AS error_count,
                CASE
                    WHEN COUNT(response.status_code) = 0 THEN 0
                    ELSE ROUND( 100.0 * COUNT_IF(response.status_code >= 400) / COUNT(response.status_code), 2 )
                END
                    AS error_rate_pct,
                ROUND( AVG( CAST( response.latency_ms AS DOUBLE ) ), 2 ) AS avg_latency_ms,
                MIN(response.latency_ms) AS min_latency_ms,
                APPROX_PERCENTILE( response.latency_ms, 0.95 ) AS p95_latency_ms,
                MAX(response.latency_ms) AS max_latency_ms,
                COALESCE( SUM(request.request_bytes), 0 ) AS total_request_bytes,
                COALESCE( SUM(response.response_bytes), 0 ) AS total_response_bytes
            FROM {SILVER_TABLE}
            WHERE
                year = '{TARGET_YEAR}'
                AND month = '{TARGET_MONTH}'
                AND day = '{TARGET_DAY}'
            GROUP BY
                domain,
                event_type,
                COALESCE(service.name, 'unknown')
        """,

        database=DATABASE_NAME,
        output_location=QUERY_RESULT_S3,
        aws_conn_id=AWS_CONN_ID,
    )


    # =====================================================
    # 4. 의존성
    # =====================================================

    t1_drop_gold_table >> t2_delete_gold_s3 >> t3_create_gold_with_ctas