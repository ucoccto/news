# =========================================================
# DAG 2. Silver -> Gold (운영형 / Partition + INSERT INTO)
# =========================================================
#
# [의도]
# Gold 테이블은 유지하고, Silver의 하루 데이터를 날짜 Partition 단위로
# Gold에 누적 저장한다.
#
# 흐름:
#   Silver(silver_logs_tbl)
#       ↓
#   T1. Gold 외부 테이블 생성 (최초 1회)
#       ↓
#   T2. 처리 날짜 Partition 메타데이터 삭제
#       ↓
#   T3. 처리 날짜 Gold S3 파일 삭제
#       ↓
#   T4. Silver 집계 → Gold INSERT INTO
#
# 결과:
# gold/daily_report/
#   └── year=2026/month=08/day=26/
#
# 수동 테스트:
# {
#   "target_date": "2026-08-26"
# }
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

# Silver / Gold 실제 데이터가 저장되는 S3 버킷
BUCKET_NAME = "de-ai-25-loggen-s3-bk-827913617635"

# silver_logs_tbl이 등록된 Glue/Athena Database
DATABASE_NAME = "de_ai_25_loggen_silver_glue_db"

SILVER_TABLE = "silver_logs_tbl"
GOLD_TABLE = "gold_daily_report_tbl"

# Athena SQL 실행 결과 저장 위치
QUERY_RESULT_S3 = f"s3://{BUCKET_NAME}/athena/dags/results/"

# Gold 실제 Parquet 데이터 저장 루트
GOLD_ROOT_PREFIX = "gold/daily_report"

# =========================================================
# 2. 처리 대상 날짜
# =========================================================

# TARGET_DATE = "{{ dag_run.conf.get('target_date', ds) }}"
# TARGET_YEAR = "{{ dag_run.conf.get('target_date', ds)[0:4] }}"
# TARGET_MONTH = "{{ dag_run.conf.get('target_date', ds)[5:7] }}"
# TARGET_DAY = "{{ dag_run.conf.get('target_date', ds)[8:10] }}"

TARGET_DATE = "2026-08-26"
TARGET_YEAR = "2026"
TARGET_MONTH = "08"
TARGET_DAY = "26"

# 예: gold/daily_report/year=2026/month=08/day=26/
GOLD_PARTITION_PREFIX = (
    GOLD_ROOT_PREFIX
    + "/year=" + TARGET_YEAR
    + "/month=" + TARGET_MONTH
    + "/day=" + TARGET_DAY
    + "/"
)
print("GOLD_PARTITION_PREFIX", GOLD_PARTITION_PREFIX)

# =========================================================
# 3. DAG 정의
# =========================================================

with DAG(
    dag_id="21_silver_to_gold_daily_report",
    description="Silver 로그를 Athena로 집계하여 날짜 Partition 단위 Gold 생성",
    default_args={
        "owner": "de_ai_25",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 8, 25, tz="Asia/Seoul"),
    catchup=False,
    tags=["aws", "airflow", "athena", "silver", "gold", "partition"],
) as dag:

    # =====================================================
    # T1. Gold 외부 테이블 생성
    # - 최초 실행 시 생성
    # - 이후에는 기존 테이블 유지
    # =====================================================

    t1_create_gold_table = AthenaOperator(
        task_id="create_gold_table",
        query=f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {GOLD_TABLE} (
                report_date DATE,
                domain STRING,
                event_type STRING,
                service_name STRING,
                total_count BIGINT,
                response_count BIGINT,
                success_count BIGINT,
                error_count BIGINT,
                error_rate_pct DOUBLE,
                avg_latency_ms DOUBLE,
                min_latency_ms BIGINT,
                p95_latency_ms BIGINT,
                max_latency_ms BIGINT,
                total_request_bytes BIGINT,
                total_response_bytes BIGINT
            )
            PARTITIONED BY (
                year STRING,
                month STRING,
                day STRING
            )
            STORED AS PARQUET
            LOCATION 's3://{BUCKET_NAME}/{GOLD_ROOT_PREFIX}/'
        """,
        database=DATABASE_NAME,
        output_location=QUERY_RESULT_S3,
        aws_conn_id=AWS_CONN_ID,
    )

    # =====================================================
    # T2. 처리 날짜의 기존 Partition 메타데이터 삭제
    # - 동일 날짜 DAG 재실행 시 중복 방지
    # =====================================================

    t2_drop_partition = AthenaOperator(
        task_id="drop_gold_partition",
        query=f"""
            ALTER TABLE {GOLD_TABLE}
            DROP IF EXISTS PARTITION (
                year='{TARGET_YEAR}',
                month='{TARGET_MONTH}',
                day='{TARGET_DAY}'
            )
        """,
        database=DATABASE_NAME,
        output_location=QUERY_RESULT_S3,
        aws_conn_id=AWS_CONN_ID,
    )

    # =====================================================
    # T3. 처리 날짜의 기존 S3 Parquet 삭제
    # - DROP PARTITION은 메타데이터만 삭제하므로
    #   실제 파일도 제거하여 재실행 가능하게 만든다.
    # =====================================================

    t3_delete_gold_s3 = S3DeleteObjectsOperator(
        task_id="delete_gold_s3_partition",
        bucket=BUCKET_NAME,
        prefix=GOLD_PARTITION_PREFIX,
        aws_conn_id=AWS_CONN_ID,
    )

    # =====================================================
    # T4. Silver -> Gold INSERT INTO
    # - domain / event_type / service.name 기준 집계
    # - 날짜별 Partition으로 Gold에 저장
    # =====================================================

    t4_insert_gold_data = AthenaOperator(
        task_id="insert_gold_daily_report",
        query=f"""
            INSERT INTO {GOLD_TABLE}
            SELECT
                DATE('{TARGET_DATE}') AS report_date,
                domain,
                event_type,
                COALESCE(service.name, 'unknown') AS service_name,
                COUNT(*) AS total_count,
                COUNT(response.status_code) AS response_count,
                COUNT_IF(
                    response.status_code >= 200
                    AND response.status_code < 400
                ) AS success_count,
                COUNT_IF(response.status_code >= 400) AS error_count,
                CASE
                    WHEN COUNT(response.status_code) = 0 THEN 0
                    ELSE ROUND(
                        100.0 * COUNT_IF(response.status_code >= 400)
                        / COUNT(response.status_code),
                        2
                    )
                END AS error_rate_pct,
                ROUND(
                    AVG(CAST(response.latency_ms AS DOUBLE)),
                    2
                ) AS avg_latency_ms,
                MIN(response.latency_ms) AS min_latency_ms,
                APPROX_PERCENTILE(
                    response.latency_ms,
                    0.95
                ) AS p95_latency_ms,
                MAX(response.latency_ms) AS max_latency_ms,
                COALESCE(
                    SUM(request.request_bytes),
                    0
                ) AS total_request_bytes,
                COALESCE(
                    SUM(response.response_bytes),
                    0
                ) AS total_response_bytes,
                '{TARGET_YEAR}' AS year,
                '{TARGET_MONTH}' AS month,
                '{TARGET_DAY}' AS day
            FROM {SILVER_TABLE}
            WHERE
                year='{TARGET_YEAR}'
                AND month='{TARGET_MONTH}'
                AND day='{TARGET_DAY}'
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
    # 4. Task 의존성
    # =====================================================

    t1_create_gold_table >> t2_drop_partition >> t3_delete_gold_s3 >> t4_insert_gold_data
