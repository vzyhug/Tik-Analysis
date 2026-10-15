# Kiến trúc ViTIED: công nghệ và cách kiểm tra log

## Công nghệ theo công đoạn

| Công đoạn | Công nghệ đang dùng | Vai trò trong project | Bằng chứng/log đã lưu |
|---|---|---|---|
| Nguồn/thu thập | Bộ dữ liệu ViTIED gồm `training_data.csv` và `streaming_data.csv`; Python audit/intake | Đọc dữ liệu đã thu thập, kiểm checksum/schema, giải canonical Post ID, đánh dấu lỗi. Hiện **không có crawler realtime**. Tên file `streaming_data.csv` không có nghĩa là một luồng đang chạy. | `outputs/quality/vitied_source_audit.json`, `vitied_g01.json`, `vitied_id_resolution.csv` |
| Lưu trữ gốc | Workspace `data/raw/vitied/` và manifest SHA-256 | Giữ CSV gốc bất biến để tái lập intake. | `data/raw/manifests/vitied_source_20261013.json`, báo cáo G01 |
| Data lake | HDFS Bronze/Silver/Gold; Parquet; NameNode + 3 DataNode, replication 2 | Bronze giữ file gốc; Silver chuẩn hóa Post/Observation/hashtag; Gold tổng hợp chỉ số cho báo cáo. Cụm này chạy trong Docker trên một host. | `outputs/quality/g02_hdfs*.json`, `g02_hdfs_fsck.txt`, `g03_bronze.json`, `g03_silver.json`, `g03_verify.json`, `g06_gold_vitied-gold-v1-20261014.json`, `g06_hdfs_fsck.txt` |
| Xử lý phân tán | Apache Spark Standalone, PySpark; 1 Master + 2 Worker + driver | Chuẩn hóa Silver, xuất graph, tạo Gold, xuất CSV. Các báo cáo job ghi task chạy ở worker nào. | `outputs/quality/g02_spark_binary.json`, `g03_silver.json`, `g04_graph_export.json`, `g06_gold_vitied-gold-v1-20261014.json`; event logs ở `outputs/spark-events/` |
| Cơ sở dữ liệu đồ thị | Neo4j Community | Nạp Post–Hashtag và chiếu đồ thị để phân tích kết nối. Sound/Topic chỉ được tạo khi có ID/nhãn đủ căn cứ. | `outputs/quality/g04_graph_load.json`; runtime log bằng `docker compose logs neo4j` |
| Huấn luyện | DL4J/Java, CPU, autoencoder embedding 64 chiều. Hai JVM worker đồng bộ tham số qua coordinator ở driver. | Mỗi JVM in `worker_start`, tiến độ batch (batch 1, mỗi 10 batch, batch cuối) và `worker_complete` trực tiếp. Trainer chuyển tiếp tức thời ra terminal với nhãn `[spark-worker-1]`/`[spark-worker-2]` và ghi cùng dòng vào file console theo model version. Đây **không phải** Spark ML executor. | Chạy `python -u scripts/train_g05_distributed.py ...`; xem terminal hoặc `outputs/quality/g05_train_<model-version>.console.txt`. JSON chỉ là báo cáo kết quả sau run, không phải log trực tiếp. |
| Phục vụ phân tích | PostgreSQL, schema `vitied`, publication tables và `vw_*` views | Lưu release Gold bất biến; views chỉ trỏ release hiện hành. Web app dùng role `web_dashboard` chỉ đọc views. | `outputs/quality/g07_postgres_vitied-gold-v1-20261014.json`; runtime log bằng `docker compose logs postgres` |
| Dashboard | FastAPI + Python; HTML/CSS/JavaScript; PostgreSQL read-only | Hiển thị KPI, trend, hashtag và snapshot. Chỉ bind `127.0.0.1:8787`; tải/làm mới chỉ đọc PostgreSQL, không chạy Spark. | `outputs/quality/g07_web_dashboard.json`; `docker compose logs web-dashboard`; mở `http://127.0.0.1:8787` |
| Điều phối môi trường | Docker Compose | Khởi động/dừng các container và nối chúng vào cùng network/volumes. | `docker compose ... ps -a`, `docker compose ... logs ...` |
| Streaming tương lai | NiFi/Kafka replay | **G08 chưa triển khai.** Nếu làm, đây là phát lại lịch sử có timestamp nguồn, không biến ViTIED thành crawl realtime. | Chưa có runtime log/bằng chứng G08. |

## Phân biệt “log chạy” và “bằng chứng kết quả”

- `docker logs`/`docker compose logs` là stdout/stderr của tiến trình trong container. Dùng chúng để xem container khởi động, lỗi kết nối, health check và trạng thái dịch vụ.
- Các job Spark/DL4J đã kết thúc. Kết quả nghiệm thu được lưu thành JSON/CSV/Markdown trong `outputs/quality/` và báo cáo `GD1.md`–`GD7.md`; đó là nơi kiểm tra số lượng, checksum, worker chạy task, model version và kết quả đối soát. Khi train, stdout/stderr của lệnh `docker compose exec` được chuyển tiếp ra terminal Python. Docker Desktop tab Logs hiển thị tiến trình chính của container; output lệnh `exec` không trở thành log PID 1, nên xem trực tiếp terminal train hoặc file console trainer tạo.
- Spark event logs ở `outputs/spark-events/` được nén, không phải file text để mở trực tiếp bằng Notepad. Project chưa cấu hình Spark History Server. Dùng JSON gate report trước; nếu cần timeline event chi tiết thì phải mở History Server hoặc giải nén event log.
- Log container cũ còn đọc được khi container dừng nếu container chưa bị xóa/recreate. `docker compose up` không tự xóa volumes; tránh `down -v`.

## Xem trạng thái và log trên PowerShell

Chạy từ thư mục gốc repo:

```powershell
# Liệt kê trạng thái mọi container trong stack chính, PostgreSQL và dashboard
docker compose -f infra/compose.yaml -f infra/compose.powerbi.yaml -f infra/compose.web.yaml ps -a

# Log gần đây của các service hạ tầng, database và dashboard
docker compose --profile graph -f infra/compose.yaml -f infra/compose.powerbi.yaml -f infra/compose.web.yaml logs --since 24h --tail 200 namenode datanode-1 datanode-2 datanode-3 spark-master spark-worker-1 spark-worker-2 spark-driver neo4j postgres web-dashboard

# Theo dõi log dịch vụ đang chạy liên tục; thoát bằng Ctrl+C
docker compose -f infra/compose.yaml -f infra/compose.powerbi.yaml -f infra/compose.web.yaml logs -f --tail 50 postgres web-dashboard

# Kiểm tra API dashboard và release hiện hành
Invoke-RestMethod http://127.0.0.1:8787/api/health
Invoke-RestMethod http://127.0.0.1:8787/api/meta
```

Tên service không có trong một compose overlay/profile có thể báo không tìm thấy. Để xem stack DL4J build riêng:

```powershell
docker compose -f infra/compose.dl4j.yaml ps -a
docker compose -f infra/compose.dl4j.yaml logs --tail 200 dl4j
```

`dl4j` là môi trường Maven/JDK. Log huấn luyện đã lưu nằm trong `outputs/quality/g05_train*.json`; service worker là `spark-worker-1` và `spark-worker-2` vì hai JVM train chạy bên trong hai container worker này.

## Trạng thái môi trường đã kiểm tra ngày 15/10/2026

- `postgres`: running/healthy, host port `5433`.
- `web-dashboard`: running/healthy, chỉ bind loopback port `8787`.
- NameNode, 3 DataNode, Spark Master, 2 Spark Worker, driver và Neo4j: hiện ở trạng thái exited/stopped; vì thế không có log “đang chạy” cho các service này lúc kiểm tra.
- Spark driver có exit code 137, nhưng Docker báo `OOMKilled=false`; không kết luận đây là OOM. Các container Hadoop/Spark/Neo4j có exit code 143, phù hợp tiến trình nhận tín hiệu dừng. Xem log container cũ và kiểm tra lại bằng `ps -a` trước khi chạy các job phụ thuộc chúng.

Chạy lại batch job cần khởi động đúng stack trước. Lệnh sau chỉ bật service, không xóa volume:

```powershell
docker compose -f infra/compose.yaml -f infra/compose.powerbi.yaml up -d
```

Thêm `--profile graph` trước các file Compose nếu cần bật Neo4j. Sau đó kiểm tra `ps -a`/health check. Chỉ gọi `spark-submit` khi Spark Master/Worker và HDFS healthy; chạy lại một job không cần thiết sẽ tốn tài nguyên và có thể tạo release mới nếu không dùng đúng publication ID.
