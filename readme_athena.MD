# 개요

| 상황                              | 과거 데이터 삭제             |
| ------------------------------- | --------------------- |
| CTAS로 항상 최신 리포트 하나만 유지          | 삭제                    |
| CTAS 결과를 매번 다른 S3 위치에 저장        | 삭제 불필요                |
| 날짜별 Gold를 장기 누적                 | 보통 CTAS보다 INSERT INTO |
| 같은 CTAS `external_location` 재사용 | 기존 S3 데이터 삭제 필요       |


|          | 1번 CTAS         | 2번 운영형       |
| -------- | --------------- | ------------ |
| Silver   | 하루 종일 계속 쌓임     | 동일           |
| DAG      | 하루 1회           | 하루 1회        |
| 실행 시점    | 보통 다음 날 00:00   | 동일           |
| 처리 데이터   | 어제 하루 Silver    | 어제 하루 Silver |
| Gold 테이블 | 매일 새로 생성        | 계속 유지        |
| 과거 Gold  | 삭제              | 유지           |
| 데이터 저장   | 최신 하루           | 날짜별 누적       |
| 목적       | 일일 Snapshot/리포트 | 장기 분석/추세     |
| 핵심 SQL   | CTAS            | INSERT INTO  |
| 실습 목적    | CTAS 개념 이해      | 실제 운영 패턴 이해  |

# 목적
- 데이터는 계속 쌓임
```
00:00 ~ 01:00 → Silver 저장
01:00 ~ 02:00 → Silver 저장
02:00 ~ 03:00 → Silver 저장
...
22:00 ~ 23:00 → Silver 저장
23:00 ~ 24:00 → Silver 저장
```
- 8월 27일 00:00이 되면 Airflow가 “8월 26일 하루치 데이터가 다 들어왔으니 이제 일별 Gold를 만들자”라고 동작하는 시나리오

- CTAS
    - 매일 오전 9시에 운영팀이 어제 시스템 상태를 보는 대시보드가 필요합니다.
    - 과거 전체 데이터 필요 없음
    - 최신 일일 리포트를 매번 새로 만든다
    
| domain    | event_type   | service          |   total | error | avg latency |
| --------- | ------------ | ---------------- | ------: | ----: | ----------: |
| ecommerce | product_view | product-service  | 530,000 | 1,200 |          42 |
| ecommerce | purchase     | order-service    | 140,000 |   850 |         121 |
| finance   | transfer     | transfer-service | 300,000 |   320 |          83 |
| gaming    | login        | auth-service     | 410,000 | 1,100 |          51 |

