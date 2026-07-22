# ETL Platform Onboarding Guide

Tài liệu này không chỉ liệt kê thư mục. Nó giải thích project như một hệ ETL production-like để một Data Engineer mới có thể hiểu: dữ liệu đi đâu, ai sở hữu phần nào, khi nào phải sửa gì, và mỗi thư mục tồn tại để giải quyết vấn đề gì.

## Mental Model

Luồng chính của platform là:

Oracle -> Raw -> STG -> SOR -> Reconcile

Orchestrator là Airflow. Compute là Spark. Storage zone là MinIO. Metadata và control plane được giữ tách biệt để mô phỏng production:

- Oracle giữ dữ liệu nguồn.
- MinIO giữ các lớp dữ liệu theo zone.
- Spark thực thi ingestion và transformation.
- Airflow điều phối, lên lịch, và theo dõi pipeline.
- PostgreSQL giữ metadata của Airflow.

Các thư mục trong repo phản ánh đúng các responsibility đó. Mỗi vùng có mục tiêu riêng để tránh trộn logic, trộn dữ liệu, hoặc trộn runtime artifact với source code.

---

## 1. Root files

### 1.1 [docker-compose.yml](../docker-compose.yml)

#### 1. Chức năng
File này là điểm khởi động chính của toàn bộ môi trường local. Nó định nghĩa toàn bộ service, network, volume, healthcheck, dependency order, và biến môi trường runtime.

Nếu bỏ file này đi, project mất đi một nguồn sự thật duy nhất cho cách các container được nối với nhau. Khi đó mỗi người sẽ tự dựng stack theo cách riêng, rất dễ lệch version, lệch port, hoặc lệch thứ tự khởi tạo.

#### 2. Trong dự án thực tế sẽ chứa gì

- Oracle service để làm source system.
- PostgreSQL service cho Airflow metadata.
- MinIO service cho data lake zone.
- minio-mc init service để tạo bucket.
- Spark master và worker.
- Airflow init, webserver, scheduler, triggerer.

#### 3. Khi nào developer sẽ làm việc với nó

- Khi thêm service mới.
- Khi đổi port, volume mount, network, hoặc healthcheck.
- Khi cần nối thêm dependency giữa Airflow, Spark, Oracle, và MinIO.
- Khi muốn thêm môi trường dev/ci/prod-like.

#### 4. Quan hệ với các thư mục khác

docker-compose.yml

-> build image từ

-> docker/

-> mount code từ

-> spark/, airflow/, oracle/, minio/, data/, logs/

-> chạy bootstrap từ

-> oracle/init, oracle/sql/init, minio/init

#### 5. Ví dụ thực tế

Nếu thêm một pipeline mới cho bảng TBAADM_GAM, file này thường không cần đổi nhiều. Nhưng nếu job mới cần thêm service phụ trợ, ví dụ metadata DB riêng hoặc một bucket mới, thì compose là nơi khai báo.

#### 6. Ai thường làm việc với nó

DevOps và Data Platform Engineer là người chỉnh nhiều nhất. Data Engineer có thể chạm vào khi cần chạy local hoặc khi cần nối Spark job vào container runtime.

### 1.2 [README.md](../README.md)

#### 1. Chức năng
Đây là tài liệu cấp cao nhất cho toàn repo. Nó mô tả system architecture, service map, startup/shutdown flow, debug commands, và cách sử dụng stack.

#### 2. Khi nào dùng

- Khi onboarding người mới.
- Khi muốn hiểu toàn cảnh trước khi chạm code.
- Khi cần xem lệnh start/stop/reset nhanh.

#### 3. Không nên đặt gì ở đây

- Không đặt thiết kế chi tiết theo từng pipeline.
- Không đặt code snippets dài hoặc operator runbook quá chi tiết.

---

## 2. docker/

### 2.1 [docker/airflow/Dockerfile](../docker/airflow/Dockerfile)

#### 1. Chức năng
Build image cho Airflow runtime. Image này cài thêm provider cho Spark, Amazon S3/MinIO, Oracle, cùng các package ETL thường dùng.

#### 2. Tại sao phải tồn tại
Airflow bản gốc không đủ dependency cho platform này. Nếu bỏ Dockerfile riêng, Airflow container sẽ thiếu provider và không thể submit Spark job, nói chuyện với Oracle, hay đọc MinIO qua S3 API.

#### 3. Trong thực tế sẽ chứa gì

- Các package pip liên quan đến Airflow provider.
- Runtime dependencies như JRE headless.

#### 4. Khi nào developer làm việc với nó

- Khi thêm provider mới.
- Khi đổi version Airflow.
- Khi DAG cần operator hoặc hook mới.

#### 5. Không nên đặt gì ở đây

- Không đặt DAG logic.
- Không đặt business transformation.
- Không đặt file cấu hình runtime riêng lẻ của từng job.

#### 6. Ví dụ thực tế

Nếu Airflow cần thêm provider cho Slack, Oracle advanced hook, hoặc một sensor mới, đây là nơi mở rộng.

### 2.2 [docker/spark/Dockerfile](../docker/spark/Dockerfile)

#### 1. Chức năng
Build Spark image cho ETL workload. Image này mang theo PySpark, boto3, pyarrow, pandas, delta-spark, JDBC driver cho Oracle, và Hadoop AWS jars cho MinIO/S3A.

#### 2. Tại sao phải tồn tại
Spark job của dự án không chỉ chạy phép transform nội bộ. Nó phải đọc Oracle, ghi MinIO, và có khả năng hỗ trợ Delta hoặc các format mở rộng khác. Nếu không đóng gói sẵn dependency, developer sẽ phải cài tay trên từng container hoặc từng máy local.

#### 3. Trong thực tế sẽ chứa gì

- System packages như curl, unzip.
- Python packages cho ETL.
- Runtime jars: ojdbc11, hadoop-aws, aws-java-sdk-bundle.

#### 4. Khi nào developer làm việc với nó

- Khi job cần thư viện mới.
- Khi đổi version Spark hoặc Oracle JDBC.
- Khi thêm connector mới.

#### 5. Không nên đặt gì ở đây

- Không đặt code job theo bảng.
- Không đặt dữ liệu thật.
- Không đặt secrets.

#### 6. Ví dụ thực tế

Nếu table TBAADM_GAM cần đọc trực tiếp từ Oracle bằng JDBC partitioning, Dockerfile này phải đảm bảo driver và classpath tương thích.

---

## 3. compose/

### 3.1 [compose/README.md](../compose/README.md)

#### 1. Chức năng
Giải thích chiến lược chia compose file trong tương lai. Hiện tại root docker-compose.yml là entry point chính, còn folder này là reserved space cho base/override patterns.

#### 2. Tại sao phải tồn tại
Ở môi trường production-like, tách compose giúp sau này dễ có dev, ci, prod-like, hoặc observability override mà không phải sửa một file khổng lồ.

#### 3. Khi nào developer làm việc với nó

- Khi tái cấu trúc stack thành nhiều lớp compose.
- Khi cần tách service theo profile.

#### 4. Không nên đặt gì ở đây

- Không đặt source code.
- Không đặt tài liệu nghiệp vụ của ETL.

#### 5. Ví dụ thực tế

Nếu sau này cần một file override chỉ để tăng số Spark worker cho load test, compose/ sẽ là nơi hợp lý.

---

## 4. spark/

Spark là vùng compute của pipeline. Nó chứa code xử lý theo zone, config runtime, shared helpers, và nơi lưu log thực thi.

### 4.1 [spark/config/spark-defaults.conf](../spark/config/spark-defaults.conf)

#### Chức năng
Là cấu hình mặc định cho Spark runtime: master URL, event log, shuffle partitions, serializer, S3A/MinIO integration, Delta support.

#### Vai trò trong pipeline
Spark dùng file này để biết kết nối tới Spark master, nơi ghi event log, và cách truy cập MinIO.

#### Khi nào dùng

- Khi tune performance.
- Khi đổi bucket endpoint.
- Khi bật/tắt Delta hoặc thay đổi session defaults.

#### Ai làm việc với nó

Data Platform Engineer và Data Engineer có trách nhiệm hiệu chỉnh.

#### Không nên đặt gì

- Không đặt logic business.
- Không đặt secret hardcode.

#### Ví dụ thực tế

Nếu job SOR cho TBAADM_GAM cần tăng shuffle partitions do volume lớn, đây là một trong các điểm tinh chỉnh.

### 4.2 [spark/config/log4j2.properties](../spark/config/log4j2.properties)

#### Chức năng
Quy định format và mức log cho Spark runtime.

#### Vai trò
Tách log console rõ ràng để debug job trong container và trong logs mount ra host.

#### Khi nào dùng

- Khi cần giảm noise log.
- Khi debug driver/executor behavior.

#### Không nên đặt gì

- Không dùng để viết business alerting logic.

### 4.3 [spark/jobs/__init__.py](../spark/jobs/__init__.py)

#### Chức năng
Đánh dấu package gốc cho toàn bộ Spark jobs.

#### Vai trò
Cho phép import nội bộ theo package path như jobs.utils.spark_session.

#### Khi nào dùng

- Khi xây dựng module hóa job.
- Khi chạy spark-submit trong package mode.

### 4.4 [spark/jobs/common/__init__.py](../spark/jobs/common/__init__.py) và [spark/jobs/common/constants.py](../spark/jobs/common/constants.py)

#### Chức năng
Giữ hằng số dùng chung cho nhiều job.

#### Trong dự án thực tế sẽ chứa gì

- Zone name.
- Tên bucket chuẩn.
- Tên source system.
- Các tag hoặc literal dùng lặp lại.

#### Khi nào developer làm việc với nó

- Khi nhiều job cùng dùng một naming convention.
- Khi đổi tên bucket hoặc zone.

#### Không nên đặt gì

- Không đặt logic transform theo từng bảng.

#### Ví dụ thực tế

Nếu team quyết định đổi cách đặt zone từ raw/stg/sor sang raw_landing/staging/warehouse, constants là nơi đầu tiên cần cập nhật.

### 4.5 [spark/jobs/config/__init__.py](../spark/jobs/config/__init__.py) và [spark/jobs/config/job_config.py](../spark/jobs/config/job_config.py)

#### Chức năng
Giữ cấu hình job-level ở dạng typed object.

#### Vai trò
JobConfig là lớp contract nhỏ cho source system và zone bucket names.

#### Khi nào dùng

- Khi job cần metadata để quyết định nguồn và đích.
- Khi chuẩn hóa config giữa các job.

#### Không nên đặt gì

- Không đặt secrets hardcoded.
- Không đặt mapping chi tiết theo cột.

### 4.6 [spark/jobs/utils/__init__.py](../spark/jobs/utils/__init__.py) và [spark/jobs/utils/spark_session.py](../spark/jobs/utils/spark_session.py)

#### Chức năng
Tạo SparkSession chuẩn hóa cho platform.

#### Vai trò trong pipeline
Tất cả Spark job nên đi qua builder này để có cùng MinIO endpoint, S3A config, và session defaults.

#### Khi nào dùng

- Khi tạo job mới.
- Khi cần chỉnh session-level config.

#### Không nên đặt gì

- Không nhét business logic vào Spark session factory.

#### Ví dụ thực tế

Job nạp TBAADM_GAM từ Oracle sang raw, rồi job raw_to_stg, rồi job stg_to_sor đều nên dùng cùng helper này để tránh mỗi job cấu hình S3A một kiểu.

### 4.7 [spark/jobs/migration/__init__.py](../spark/jobs/migration/__init__.py) và [spark/jobs/migration/oracle_to_raw.py](../spark/jobs/migration/oracle_to_raw.py)

#### Chức năng
Chứa job trích xuất từ Oracle sang zone raw.

#### Vai trò trong pipeline
Đây là lớp ingestion đầu tiên. Mục tiêu là lấy dữ liệu nguồn về raw càng sát nguồn càng tốt, ít biến đổi nhất có thể.

#### Khi nào developer làm việc với nó

- Khi thêm bảng nguồn mới.
- Khi đổi logic extract từ full load sang incremental.
- Khi thêm CDC logic hoặc partitioned JDBC read.

#### Không nên đặt gì

- Không làm business enrichment nặng.
- Không merge logic thuộc STG hoặc SOR.

#### Ví dụ thực tế

Với TBAADM_GAM, job này sẽ đọc bảng Oracle, ghi xuống MinIO raw theo partition date, giữ gần như nguyên trạng để audit và replay.

### 4.8 [spark/jobs/stg/__init__.py](../spark/jobs/stg/__init__.py) và [spark/jobs/stg/raw_to_stg.py](../spark/jobs/stg/raw_to_stg.py)

#### Chức năng
Chứa job biến đổi dữ liệu từ raw sang STG.

#### Vai trò
STG là lớp chuẩn hóa dữ liệu. Tại đây thường xảy ra clean-up, dedup, cast kiểu, chuẩn format ngày giờ, chuẩn hóa null, và mapping theo chuẩn kỹ thuật.

#### Khi nào developer làm việc với nó

- Khi có mapping raw -> STG mới.
- Khi thêm rule chuẩn hóa cột.
- Khi cần đổi logic schema evolution.

#### Không nên đặt gì

- Không đặt enrichment nghiệp vụ quá sâu.
- Không đặt logic đối soát.

#### Ví dụ thực tế

TBAADM_GAM có thể có cột ngày ở nhiều format từ Oracle. STG là nơi chuẩn hóa toàn bộ sang một format chuẩn cho downstream.

### 4.9 [spark/jobs/sor/__init__.py](../spark/jobs/sor/__init__.py) và [spark/jobs/sor/stg_to_sor.py](../spark/jobs/sor/stg_to_sor.py)

#### Chức năng
Chứa job đẩy dữ liệu từ STG sang SOR.

#### Vai trò
SOR là lớp curated/warehouse-friendly. Đây là nơi data model bắt đầu phục vụ tiêu dùng phân tích hoặc downstream domain.

#### Khi nào developer làm việc với nó

- Khi có mapping STG -> SOR mới.
- Khi cần upsert/merge theo business key.
- Khi thay đổi model dimensional hoặc curated.

#### Không nên đặt gì

- Không giữ raw duplicates hay noise kỹ thuật.

#### Ví dụ thực tế

Nếu TBAADM_GAM là source table, SOR có thể tách ra các trường phục vụ báo cáo tài chính hoặc quản trị danh mục tín dụng.

### 4.10 [spark/jobs/reconcile/__init__.py](../spark/jobs/reconcile/__init__.py) và [spark/jobs/reconcile/sor_reconcile.py](../spark/jobs/reconcile/sor_reconcile.py)

#### Chức năng
Chứa job đối soát và tạo audit summary giữa các lớp hoặc so với kỳ vọng nguồn.

#### Vai trò
Reconcile dùng để chứng minh rằng số lượng bản ghi, tổng số tiền, checksum, hay rule chất lượng data khớp sau transform.

#### Khi nào developer làm việc với nó

- Khi cần kiểm tra data quality.
- Khi bổ sung business validation.
- Khi dashboard audit hoặc control report cần thêm chỉ tiêu.

#### Không nên đặt gì

- Không đặt transformation chính.
- Không thay thế hoàn toàn unit test.

#### Ví dụ thực tế

Sau khi load TBAADM_GAM vào SOR, reconcile có thể ghi lại row count, count theo trạng thái, hoặc tổng giá trị exposure để so với nguồn.

### 4.11 Data flow nội bộ của spark/

spark/jobs/migration/oracle_to_raw.py

-> đọc Oracle

-> ghi MinIO raw

spark/jobs/stg/raw_to_stg.py

-> đọc raw

-> chuẩn hóa và ghi STG

spark/jobs/sor/stg_to_sor.py

-> đọc STG

-> ghi SOR

spark/jobs/reconcile/sor_reconcile.py

-> đọc SOR

-> tạo summary audit

### 4.12 spark/jars/

Thư mục này chưa xuất hiện trong tree hiện tại như một folder riêng, nhưng conceptually đây là nơi để giữ các jar runtime nếu sau này muốn quản trị tường minh thay vì tải trong Dockerfile.

#### Khi nào cần thêm

- Khi muốn pin jar vào repo hoặc build context.
- Khi muốn review dependency rõ ràng hơn.

#### Không nên đặt gì

- Không đặt source code job.

### 4.13 spark/logs/

Hiện log Spark được mount qua [logs/spark](../logs/spark) trên host. Nếu tương lai muốn tách log nội bộ cho Spark code, đây sẽ là khu vực logical tương ứng.

### 4.14 spark/warehouse/

Chưa có trong tree hiện tại. Nếu sau này dùng Spark SQL warehouse nội bộ hoặc metastore-backed warehouse, thư mục này thường là nơi lưu warehouse path hoặc artifact phụ trợ.

---

## 5. airflow/

Airflow là lớp điều phối. Nó không transform data; nó quyết định khi nào chạy, chạy cái gì, và theo thứ tự nào.

### 5.1 [airflow/dags/migration_dag.py](../airflow/dags/migration_dag.py)

#### Chức năng
Định nghĩa DAG cho luồng Oracle -> Raw.

#### Vai trò
Đây là DAG đầu tiên trong pipeline ingestion.

#### Khi nào developer làm việc với nó

- Khi thêm bảng nguồn.
- Khi gắn SparkSubmitOperator hoặc task thực thi thật.
- Khi đổi schedule hoặc dependency.

#### Không nên đặt gì

- Không đặt logic transform chi tiết.

### 5.2 [airflow/dags/stg_dag.py](../airflow/dags/stg_dag.py)

#### Chức năng
Điều phối job raw -> stg.

#### Vai trò
Đây là lớp chuẩn hóa đầu tiên sau ingestion.

### 5.3 [airflow/dags/sor_dag.py](../airflow/dags/sor_dag.py)

#### Chức năng
Điều phối job stg -> sor.

#### Vai trò
Quản lý load vào lớp curated/warehouse-friendly.

### 5.4 [airflow/dags/reconcile_dag.py](../airflow/dags/reconcile_dag.py)

#### Chức năng
Điều phối job reconcile.

#### Vai trò
Chạy kiểm chứng chất lượng và tạo audit summary sau khi load.

### 5.5 [airflow/config/airflow_local_settings.py](../airflow/config/airflow_local_settings.py)

#### Chức năng
Nơi đặt custom policy, logging behavior, hoặc plugin hooks cho Airflow.

#### Khi nào dùng

- Khi cần governance rule.
- Khi muốn tùy biến log format hoặc policy.

#### Không nên đặt gì

- Không đặt DAG business logic.

### 5.6 airflow/plugins/

#### Chức năng
Nơi dành cho plugin hoặc extension của Airflow.

#### Khi nào dùng

- Khi cần custom operator, hook, hoặc UI extension.

#### Không nên đặt gì

- Không nhét toàn bộ DAG code vào đây.

### 5.7 airflow/logs/

#### Chức năng
Lưu log Airflow scheduler, webserver, task instances.

#### Khi nào dùng

- Khi debug DAG import.
- Khi kiểm tra task failure.

### 5.8 Airflow scan DAG như thế nào

Airflow scheduler sẽ quét thư mục DAG mount vào /opt/airflow/dags. Trong compose hiện tại, đó là bind mount từ [airflow/dags](../airflow/dags). Khi thêm file .py mới ở đây, scheduler sẽ parse lại theo chu kỳ và hiển thị DAG mới trong UI nếu file import hợp lệ.

### 5.9 Quan hệ với các thư mục khác

airflow/dags/*.py

-> gọi Spark job trong spark/jobs

-> dùng settings từ airflow/config

-> ghi log vào airflow/logs

-> phụ thuộc runtime image từ docker/airflow/Dockerfile

---

## 6. oracle/

Oracle là source system. Khu này tách riêng để mô phỏng nguồn dữ liệu doanh nghiệp và khởi tạo schema nguồn một cách deterministic.

### 6.1 [oracle/sql/init/00_init_schema.sql](../oracle/sql/init/00_init_schema.sql)

#### Chức năng
DDL khởi tạo schema nguồn và bảng audit cơ bản trên Oracle.

#### Vai trò
Đây là dữ liệu nguồn đầu vào cho pipeline. Oracle container sẽ tự chạy script trong lần boot đầu tiên.

#### Khi nào developer làm việc với nó

- Khi thêm source table mới.
- Khi cần seed schema test.
- Khi muốn tái tạo local source database từ đầu.

#### Không nên đặt gì

- Không đặt ETL transform logic.
- Không đặt Spark code.

### 6.2 [oracle/init/README.md](../oracle/init/README.md)

#### Chức năng
Ghi chú về cách Oracle init hoạt động và thứ tự đặt file DDL.

#### Vai trò
Là tài liệu bootstrap cho team.

### 6.3 Oracle initialization order

Oracle container đọc toàn bộ .sql trong [oracle/sql/init](../oracle/sql/init) theo thứ tự lexicographic. Vì vậy file phải được đặt tên có prefix số:

00_init_schema.sql

01_create_source_tables.sql

02_seed_data.sql

#### Nếu bỏ quy ước này

Thứ tự tạo object có thể sai, ví dụ bảng con được tạo trước bảng cha hoặc seed data chạy trước DDL.

### 6.4 Ai làm việc với nó

Data Engineer, Data Platform Engineer, và đôi khi DBA/DevOps nếu cần thay đổi schema nguồn mô phỏng.

---

## 7. minio/

MinIO là object storage mô phỏng data lake. Mỗi zone tương ứng một bucket hoặc prefix theo convention.

### 7.1 [minio/init/init-buckets.sh](../minio/init/init-buckets.sh)

#### Chức năng
Script bootstrap tạo bucket, set policy, và pre-create layout theo convention.

#### Vai trò
Chạy một lần khi stack khởi động, đảm bảo vùng storage sẵn sàng trước khi Spark hoặc Airflow ghi dữ liệu.

#### Khi nào developer làm việc với nó

- Khi thêm bucket mới.
- Khi đổi naming convention.
- Khi cần set policy cho bucket.

#### Không nên đặt gì

- Không đặt ETL logic.
- Không đặt dữ liệu thật.

#### Ví dụ thực tế

Nếu thêm zone archive hoặc quarantine, script này là nơi tạo bucket đó và set policy tương ứng.

### 7.2 [minio/bucket/README.md](../minio/bucket/README.md)

#### Chức năng
Mô tả quy ước bucket và prefix cho data lake zones.

#### Vai trò
Là tài liệu giao ước giữa team Data, DevOps, và BI.

### 7.3 Bucket lifecycle

Bucket được tạo khi container minio-mc chạy thành công và kết nối tới MinIO healthy. Điều này xảy ra sau khi MinIO server sẵn sàng, trước khi Spark hoặc Airflow thực thi job phụ thuộc storage.

### 7.4 Quan hệ với data flow

Spark ghi vào s3a://raw, s3a://stg, s3a://sor, s3a://reconcile tương ứng qua S3A endpoint trỏ tới MinIO.

---

## 8. scripts/

Script là lớp operational convenience. Không nên nhầm chúng với business code.

### 8.1 [scripts/start.ps1](../scripts/start.ps1)

#### Chức năng
Start toàn bộ stack bằng Docker Compose.

#### Loại script
Script chạy mỗi lần start.

#### Khi nào dùng

- Khi developer mở môi trường local.
- Khi cần rebuild image và khởi động lại toàn bộ platform.

### 8.2 [scripts/stop.ps1](../scripts/stop.ps1)

#### Chức năng
Stop stack nhưng giữ volume.

#### Loại script
Script chạy mỗi lần dừng.

### 8.3 [scripts/reset.ps1](../scripts/reset.ps1)

#### Chức năng
Tắt stack, xoá volume, và dựng lại từ đầu.

#### Loại script
Script destructive dùng khi cần clean slate.

#### Khi nào dùng

- Khi Oracle init thay đổi.
- Khi bucket bootstrap cần chạy lại sạch.
- Khi cần reset state cho demo hoặc test.

### 8.4 [scripts/spark-submit-sample.ps1](../scripts/spark-submit-sample.ps1)

#### Chức năng
Là lệnh mẫu để submit một Spark job từ host vào Spark master.

#### Loại script
Script debug / manual run.

#### Khi nào dùng

- Khi muốn test job nhanh mà chưa nối Airflow.
- Khi debug transformation ngoài DAG.

### 8.5 Phân loại script theo vòng đời

- Chạy một lần: bootstrap init script bên trong minio/init hoặc oracle/sql/init.
- Chạy mỗi lần start: scripts/start.ps1.
- Chạy mỗi lần stop: scripts/stop.ps1.
- Chạy khi cần clean state: scripts/reset.ps1.
- Chạy để debug: scripts/spark-submit-sample.ps1.

---

## 9. data/

data là vùng làm việc cho dữ liệu local. Nó giúp team quan sát artifacts của từng zone và debug pipeline mà không phải vào container.

### 9.1 [data/raw](../data/raw)

#### Chức năng
Chứa dữ liệu thô từ Oracle hoặc nguồn đầu vào tương tự.

#### Lifecycle
Oracle -> Raw.

#### Khi nào dùng

- Sau job migration hoàn tất.
- Khi kiểm tra dữ liệu gốc đã được ingest đúng chưa.

#### Loại file phù hợp

- Parquet thô.
- CSV landing nếu có.
- Partitioned folders theo ngày hoặc table.

#### Không nên đặt gì

- Không đặt dữ liệu đã chuẩn hóa.
- Không đặt dữ liệu nghiệp vụ đã join nhiều nguồn.

### 9.2 [data/stg](../data/stg)

#### Chức năng
Chứa dữ liệu đã chuẩn hóa và làm sạch ở mức kỹ thuật.

#### Lifecycle
Raw -> STG.

#### Khi nào dùng

- Khi kiểm tra dedup, cast kiểu, chuẩn hóa schema.

### 9.3 [data/sor](../data/sor)

#### Chức năng
Chứa dữ liệu curated, sẵn sàng cho downstream analytics hoặc modeling.

#### Lifecycle
STG -> SOR.

#### Khi nào dùng

- Khi kiểm tra logic business key, upsert, hoặc dimensional layout.

### 9.4 [data/reconcile](../data/reconcile)

#### Chức năng
Chứa kết quả đối soát, audit snapshot, và indicator chất lượng.

#### Lifecycle
SOR -> Reconcile.

#### Khi nào dùng

- Khi cần chứng minh pipeline load đủ dữ liệu.

### 9.5 [data/tmp](../data)

Thư mục tmp chưa có trong tree hiện tại nhưng là chuẩn rất nên có cho scratch space, intermediate dumps, hoặc files staging ngắn hạn.

#### Không nên dùng tmp cho gì

- Không để dữ liệu chính thức.
- Không để output cuối cùng của pipeline.

### 9.6 [logs/](../logs)

Trong tree hiện tại, logs là thư mục host-level riêng. Nó chứa log runtime của các service thay vì data nghiệp vụ.

#### Các nhánh đang có

- [logs/airflow](../logs/airflow)
- [logs/minio](../logs/minio)
- [logs/oracle](../logs/oracle)
- [logs/spark](../logs/spark)

#### Ý nghĩa production-like

Tách logs khỏi data giúp tránh nhầm lẫn giữa artifact vận hành và dữ liệu nguồn/đích.

### 9.7 Lifecycle tổng quát của data

Oracle

-> Raw

-> STG

-> SOR

-> Reconcile

-> Archive nếu cần

Mỗi lớp có mục tiêu riêng:

- Raw giữ nguyên trạng để replay và audit.
- STG chuẩn hóa kỹ thuật.
- SOR phục vụ phân tích và nghiệp vụ.
- Reconcile chứng minh độ tin cậy của pipeline.

---

## 10. docs/

### 10.1 [docs/architecture.md](../docs/architecture.md)

#### Chức năng
Tài liệu kiến trúc cấp cao của platform.

#### Vai trò
Là bản tóm tắt ngắn gọn cho end-to-end flow và scale path.

### 10.2 [docs/onboarding-architecture.md](./onboarding-architecture.md)

#### Chức năng
Tài liệu onboarding chi tiết, production-oriented, dùng cho DE mới vào team.

#### Vai trò
Là tài liệu vận hành và giải thích thư mục theo góc nhìn trách nhiệm hệ thống.

### 10.3 Nên lưu gì trong docs/

- Kiến trúc hệ thống.
- Data flow.
- Quy ước naming.
- Runbook.
- Deployment notes.
- Data contract hoặc mapping summary.

### 10.4 Không nên lưu gì trong docs/

- Secret.
- Dump dữ liệu.
- File log runtime lớn.
- Mã nguồn ETL chi tiết.

---

## 11. init/

### 11.1 [init/README.md](../init/README.md)

#### Chức năng
Giữ chỗ cho bootstrap dùng chung ở cấp platform.

#### Vai trò
Khi cần các bước init không thuộc riêng Oracle hay MinIO, chúng có thể đặt ở đây.

#### Khi nào dùng

- Khi thêm bootstrap script cross-service.
- Khi muốn orchestration init theo thứ tự rõ ràng.

#### Không nên đặt gì

- Không đặt logic service-specific nếu đã có oracle/init hoặc minio/init.

---

## 12. jobs/

### 12.1 jobs/README.md

Trong tree hiện tại không thấy file README cụ thể trong jobs/, nhưng về mặt kiến trúc đây là vùng reserved để mô tả các job-level contract hoặc job catalog nếu team muốn tách khỏi spark/jobs.

#### Khuyến nghị

- Nếu dùng, hãy đặt tài liệu job inventory hoặc execution notes.
- Không nên đưa code Spark vào đây nếu code đã nằm trong spark/jobs.

### 12.2 Vai trò của jobs/ trong hệ thống

Đây là tên gọi cấp khái niệm cho lớp ETL logic. Tuy nhiên source code thực tế của platform đang nằm trong [spark/jobs](../spark/jobs). Vì vậy jobs/ ở root hiện tại là vùng khái niệm hoặc mở rộng tương lai.

---

## 13. shared/

### 13.1 [shared/README.md](../shared/README.md)

#### Chức năng
Giữ chỗ cho tài sản dùng chung giữa Spark, Airflow, và các thành phần khác.

#### Trong dự án thực tế sẽ chứa gì

- Data contract.
- Schema definitions.
- Shared SQL templates.
- Mapping files.
- Tài liệu chuẩn naming.

#### Khi nào developer làm việc với nó

- Khi nhiều pipeline dùng cùng contract.
- Khi mapping STG -> SOR được tái sử dụng.
- Khi muốn tách logic cấu hình khỏi code job.

#### Không nên đặt gì

- Không đặt code specific cho một bảng.
- Không đặt raw artifacts.

#### Ví dụ thực tế

Nếu TBAADM_GAM và một bảng khác dùng cùng chuẩn mã chi nhánh, currency, hoặc ngày hiệu lực, contract chung có thể nằm ở shared/.

---

## 14. sql/

### 14.1 sql/ (root)

Thư mục sql ở root hiện đang tồn tại như một vùng khái niệm hoặc mở rộng tương lai. Vì tree hiện tại chưa có file bên trong, nó có thể được dùng cho SQL dùng chung, script review, hoặc query mẫu.

#### Nên lưu gì nếu dùng

- SQL tham chiếu chung.
- Query kiểm tra data quality.
- SQL snippets phục vụ analysis hoặc validation.

#### Không nên lưu gì

- DDL riêng của Oracle init nếu đã có oracle/sql/init.
- Business logic SQL rải rác không có tổ chức.

---

## 15. config/

### 15.1 config/ (root)

Root config hiện tại là một vùng placeholder cấp platform. Nếu sau này có config dùng chung cho app, environment template, hoặc conventions, thì đây là nơi hợp lý.

#### Nên lưu gì

- Cấu hình dùng chung giữa nhiều service.
- Template hoặc schema cấu hình.

#### Không nên lưu gì

- Secrets thật.
- Config chỉ dùng cho một container cụ thể nếu đã có folder service-specific.

---

## 16. logs/

### 16.1 logs/ (root)

#### Chức năng
Là vùng host-level cho log runtime tách khỏi source code.

#### Trong dự án thực tế sẽ chứa gì

- [logs/airflow](../logs/airflow)
- [logs/minio](../logs/minio)
- [logs/oracle](../logs/oracle)
- [logs/spark](../logs/spark)

#### Khi nào developer làm việc với nó

- Khi debug task failure.
- Khi inspect startup issue của container.

#### Không nên đặt gì

- Không đặt file dữ liệu thật.
- Không đặt source code.

---

## 17. Full pipeline view

### 17.1 ASCII flow

Oracle

-> extract by Spark migration job

-> MinIO/raw

-> Spark STG job

-> MinIO/stg

-> Spark SOR job

-> MinIO/sor

-> Spark reconcile job

-> MinIO/reconcile

-> Airflow orchestrates and retries each stage

### 17.2 Flow with control plane

Airflow DAG

-> trigger spark-submit or Spark operator

-> Spark reads config from spark/config

-> Spark reads/writes MinIO via S3A

-> Spark may read Oracle via JDBC

-> outputs land in data/ and logs/

---

## 18. End-to-end example: receive a new STG -> SOR mapping for TBAADM_GAM

### 18.1 What changes first

1. Update or create mapping documentation in [shared/](../shared) if the mapping is reusable or should be visible to the team.
2. Implement the transformation logic in [spark/jobs/sor/stg_to_sor.py](../spark/jobs/sor/stg_to_sor.py) or a new table-specific module under spark/jobs/sor/.
3. If the STG logic also changes, update [spark/jobs/stg/raw_to_stg.py](../spark/jobs/stg/raw_to_stg.py).
4. If the source ingestion shape changes, update [spark/jobs/migration/oracle_to_raw.py](../spark/jobs/migration/oracle_to_raw.py).
5. If the job needs new shared constants or config, update [spark/jobs/common/constants.py](../spark/jobs/common/constants.py) or [spark/jobs/config/job_config.py](../spark/jobs/config/job_config.py).
6. If the job needs a new DAG trigger or schedule, update the matching file in [airflow/dags](../airflow/dags).

### 18.2 Do I need to write Spark job code?

Yes. The production work belongs under [spark/jobs](../spark/jobs). For TBAADM_GAM, the clean approach is usually to create table-specific modules or functions under the relevant zone package.

### 18.3 Do I need to add a DAG?

If this is a new independently triggerable pipeline or a new table-specific flow, yes. The DAG belongs in [airflow/dags](../airflow/dags). If the existing DAG is only a skeleton, replace the EmptyOperator chain with a Spark submit task.

### 18.4 Do I need to create a bucket?

Usually no if the existing bucket set already includes raw, stg, sor, reconcile, logs, and tmp. You only add a bucket if you introduce a new zone or a special-purpose storage area. If you do, update [minio/init/init-buckets.sh](../minio/init/init-buckets.sh).

### 18.5 Do I need to edit Docker?

Only if the mapping introduces new runtime dependencies.

- If you need a new Python library, JDBC driver, or Spark extension, update [docker/spark/Dockerfile](../docker/spark/Dockerfile).
- If Airflow needs a new provider or hook, update [docker/airflow/Dockerfile](../docker/airflow/Dockerfile).
- If the change is only transformation logic, you usually do not touch Docker.

### 18.6 What commands should I run?

Typical workflow:

1. Start stack with [scripts/start.ps1](../scripts/start.ps1).
2. If dependencies changed, rebuild images with docker compose up -d --build.
3. Submit Spark job manually with [scripts/spark-submit-sample.ps1](../scripts/spark-submit-sample.ps1) or via Airflow DAG.
4. Check logs in [logs/spark](../logs/spark) and [logs/airflow](../logs/airflow).
5. Inspect output data under [data/sor](../data/sor) and reconciliation outputs under [data/reconcile](../data/reconcile).

### 18.7 How do I verify result?

- Check Spark task logs for schema, row count, and write completion.
- Confirm output files appear in MinIO zones or mirrored host paths.
- Confirm reconcile output exists and audit counts match expectation.
- Check Airflow UI for DAG run status.

### 18.8 How do I reconcile?

Reconcile should compare the expected and actual result of the load. For bank data, typical checks include:

- Row count before and after transform.
- Count by status or product type.
- Sum of monetary columns.
- Null rate on mandatory fields.
- Duplicate key detection.

That logic belongs under [spark/jobs/reconcile](../spark/jobs/reconcile) and is triggered by [airflow/dags/reconcile_dag.py](../airflow/dags/reconcile_dag.py).

### 18.9 What does a complete change look like?

For TBAADM_GAM, a proper change usually touches these layers:

- Oracle source schema or seed data if the source model changes.
- Migration job if extraction rules change.
- STG job if clean-up or normalization changes.
- SOR job if business mapping changes.
- Reconcile job if validation rules change.
- Airflow DAG if orchestration changes.
- Docker only if new dependencies are introduced.

---

## 19. Practical rules by role

### Data Engineer

- Works mostly in spark/jobs, airflow/dags, and shared/.
- Owns transformation logic, mappings, and validation.

### Data Platform Engineer

- Works mostly in docker/, compose/, spark/config/, airflow/config/, scripts/.
- Owns runtime stability, packaging, and orchestration conventions.

### DevOps / SRE

- Works mostly in docker-compose.yml, docker/, compose/, logs/, scripts/.
- Owns startup, healthcheck, network, volume, and observability concerns.

### Data Analyst / BI

- Usually consumes data from SOR or reconcile outputs.
- May read docs/ and shared/ to understand data meaning.

### DBA / Source System Owner

- Works in oracle/sql/init and possibly source schema conventions.

---

## 20. What should never be mixed

- Business ETL logic should not live in docker-compose.yml.
- Spark configuration should not live inside DAG files.
- Source schema DDL should not live in Spark job folders.
- Runtime logs should not be stored in data zones.
- Reconciliation summaries should not be treated as curated business datasets.

---

## 21. Summary

This repository is organized to separate four concerns:

1. Infrastructure runtime: docker/, compose/, scripts/.
2. Compute logic: spark/jobs and spark/config.
3. Orchestration: airflow/dags and airflow/config.
4. Data lifecycle: oracle/, minio/, data/, logs/, docs/, shared/.

That separation is what makes the project production-like. It also makes the platform maintainable when a new table like TBAADM_GAM lands, because you know exactly where to put source init, transformation, orchestration, and validation.
