# Runbook: pipeline và log

## Thành phần đang dùng

| Công đoạn | Công nghệ | Điểm kiểm tra |
|---|---|---|
| Intake | Python + manifest SHA-256 | `outputs/quality/vitied_g01.json` |
| Lake | HDFS Bronze/Silver/Gold, Parquet | `hdfs fsck`, các report G02/G03/G06 |
| Xử lý | Spark Standalone, 1 Master + 2 Worker | Spark UI, worker witness trong report, `outputs/spark-events/` |
| Đồ thị | Neo4j Community | `outputs/quality/g04_graph_load.json` |
| Model | Java DL4J/ND4J CPU trên 2 container worker | terminal lúc train và `outputs/quality/*.console.txt` |
| Dashboard | FastAPI + HTML/CSS/JS, đọc Neo4j | `http://127.0.0.1:8787`, `/api/health`, G07 verifier |

Docker là cụm một host, không phải HA nhiều host. Dashboard không khởi chạy Spark hay train lúc người dùng tải trang.

## Trạng thái và log Docker

Từ thư mục gốc repository:

```powershell
docker compose -f infra/compose.yaml ps -a
docker compose --profile graph -f infra/compose.yaml -f infra/compose.web.yaml ps -a
docker compose --profile graph -f infra/compose.yaml -f infra/compose.web.yaml logs --tail 100 namenode datanode-1 datanode-2 datanode-3 spark-master spark-worker-1 spark-worker-2 spark-driver neo4j web-dashboard
Invoke-RestMethod http://127.0.0.1:8787/api/health
```

`docker compose logs` xem stdout/stderr của tiến trình chính trong container. Output lệnh `docker compose exec` không thuộc PID 1; Spark submit/training cần xem terminal gọi lệnh hoặc file console do script tạo.

## Train DL4J và xem log worker

G05 dùng hai JVM DL4J CPU chạy trong `spark-worker-1` và `spark-worker-2`, coordinator ở driver. Đây là đồng bộ trọng số qua hai process, không phải Spark ML executor.

```powershell
docker compose -f infra/compose.yaml up -d namenode datanode-1 datanode-2 datanode-3 spark-master spark-worker-1 spark-worker-2 spark-driver
docker compose -f infra/compose.dl4j.yaml run --rm dl4j mvn -q package dependency:copy-dependencies
python -u scripts/train_g05_distributed.py --features outputs/g05/features/vitied-g05-caption-idf512-v3 --output outputs/g05/models/<model_version_moi> --rounds 3 --batch-size 64
```

Trainer stream log hai worker ra terminal, đồng thời lưu `outputs/quality/g05_train_<model_version>.console.txt` và JSON metric. Mỗi lần dùng `model_version`/output mới để tránh ghi đè.

## Evidence

- Gate status và đường dẫn report: [reports/progress.md](../reports/progress.md).
- JSON/CSV kiểm chứng: `outputs/quality/`.
- Spark event logs: `outputs/spark-events/` (định dạng event log, không đọc trực tiếp như text log).
- HDFS/Silver/Gold và Neo4j giữ trong Docker volumes. Không dùng `docker compose down -v`, format NameNode hoặc reset database trong vận hành bình thường.
