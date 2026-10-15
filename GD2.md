# Báo cáo G02: HDFS và Spark Standalone với ViTIED

**Kết quả:** G02 đạt gate hạ tầng trên cụm Docker một host hiện có. Hai CSV ViTIED đã được ghi vào **vùng diagnostics** của HDFS, đọc lại đúng SHA-256, `fsck` healthy với replication 2 và 3 DataNode sống. Spark Standalone cấp executor và thực thi task trên **cả hai worker**; job đọc bytes hai CSV từ HDFS khớp checksum nguồn và ghi/đọc lại Parquet diagnostics. NameNode restart xong vẫn giữ namespace và dữ liệu.

G02 kiểm chứng hạ tầng, **chưa xuất Bronze/Silver chính thức**. Khi thử parser CSV của Spark, training bị thiếu 3 dòng so với Python CSV/G01; [kết quả thử này](outputs/quality/g02_spark.json) được giữ để xử lý ở G03, không dùng làm row count đã chuẩn hóa.

## 1. Cấu hình đã dùng

| Thành phần | Cấu hình thực tế |
|---|---|
| Hadoop | Image `apache/hadoop:3.4.2`; 1 NameNode, 3 DataNode, `dfs.replication=2` |
| Spark | Image `apache/spark:4.0.2-python3`; 1 Master, 2 Worker, driver riêng |
| Worker | Mỗi worker 1 core, 768 MiB Spark memory; container limit 1.280 MiB |
| Driver | Client mode trong container `spark-driver`; Spark heap 768 MiB; container limit 1.280 MiB |
| Lưu trữ | Volume NameNode và từng DataNode riêng; không format hoặc xóa volume khi chạy G02 |
| Phạm vi | Các container cùng một Docker host; không có HA host, DL4J/Neo4j không trở thành phân tán nhờ cụm này |

NameNode được recreate **riêng container** để nhận mount `data/raw/vitied/` chỉ đọc. Namespace HDFS và `/trend/diagnostics/vitied-g02` vẫn tồn tại sau restart. Script [start-hdfs.sh](infra/hadoop/start-hdfs.sh) chỉ format khi volume mới trống và từ chối format volume có dữ liệu nhưng thiếu metadata.

## 2. Bằng chứng HDFS

Thư mục kiểm chứng: `/trend/diagnostics/vitied-g02/20261013T101756Z-04ef3318/`. Đây là bản sao để kiểm tra, không phải Bronze G03.

| File | Byte | SHA-256 nguồn và HDFS đọc lại |
|---|---:|---|
| `training_data.csv` | 16.189.863 | `370234d0358b7103e7574dd7718054e82f53ce5092a6024e1ec5ed288b8b0c10` |
| `streaming_data.csv` | 1.944.254 | `fa9fe04c9144050f34375b2cc3cfb4e9edc3a0b15b7f36af7c9476e33e8d21da` |

[Báo cáo HDFS ban đầu](outputs/quality/g02_hdfs.json) ghi hai file có checksum khớp, `fsck_status_healthy=true`, replication 2 và 3 DataNode sống. Sau `docker compose ... restart namenode`, [báo cáo đọc lại](outputs/quality/g02_hdfs_restart.json) vẫn `passed=true`. [Fsck chi tiết sau restart](outputs/quality/g02_hdfs_fsck.txt) ghi **8 block hợp lệ**, `Under-replicated blocks: 0`, `Missing blocks: 0`, mỗi block có `Live_repl=2` cùng hai vị trí DataNode; 3 DataNode hoạt động. Tám block gồm hai CSV và các file Parquet diagnostics do Spark ghi.

## 3. Bằng chứng Spark hai worker

[Job Spark đọc bytes](jobs/spark/00_verify_vitied_g02_bytes.py) chạy với application ID `app-20261013102508-0001` trên `spark://spark-master:7077`. [Báo cáo job](outputs/quality/g02_spark_binary.json) ghi:

- Hai executor trên `spark-worker-1` và `spark-worker-2`, mỗi executor 1 core/768 MiB; task thực thi trên cả hai host.
- Spark đọc đủ **16.189.863 + 1.944.254 byte** từ HDFS và tính SHA-256 khớp [manifest G01](data/raw/manifests/vitied_source_20261013.json).
- Spark ghi Parquet `spark-byte-check` vào HDFS rồi đọc lại đúng hai tên file, kích thước và checksum. Event log của application nằm trong `outputs/spark-events/`.

Lần thử [CSV parser](jobs/spark/00_verify_vitied_g02.py) cũng có task trên cả hai worker và đọc đúng **5.114** dòng streaming, nhưng training chỉ **29.633**, ít hơn **3** so với 29.636 dòng từ Python CSV/G01. Điều này **không** làm hỏng bằng chứng HDFS/Spark đọc bytes, nhưng là gate của G03: adapter phải xác định ba record khác biệt và đối soát 34.750 dòng trước khi xuất Silver. Không được dùng output `spark-check` của lần thử parser làm bảng dữ liệu chuẩn.

## 4. Lệnh tái lập

Chạy tại gốc repository sau khi Docker daemon đã khởi động. Giữ volume; không dùng `down -v` hoặc format NameNode.

```powershell
docker compose -f infra/compose.yaml config --quiet
docker compose -f infra/compose.yaml up -d spark-master spark-worker-1 spark-worker-2 spark-driver
python scripts/verify_g02_hdfs.py
```

Lệnh Python tạo đường dẫn diagnostics mới và in `hdfs_directory`; dùng đường dẫn đó cho job Spark:

```powershell
$hdfsDirectory = '/trend/diagnostics/vitied-g02/<run_id>' # thay bằng hdfs_directory vừa in
docker compose -f infra/compose.yaml exec -T spark-driver /opt/spark/bin/spark-submit /workspace/jobs/spark/00_verify_vitied_g02_bytes.py --hdfs-directory $hdfsDirectory
docker compose -f infra/compose.yaml restart namenode
python scripts/verify_g02_hdfs.py --verify-only
```

Nếu mount mới chưa có trong container NameNode cũ, chạy `docker compose -f infra/compose.yaml up -d --no-deps --force-recreate namenode` sau khi đã kiểm tra namespace/metadata volume. Script khởi động bảo vệ metadata cũ; cần đợi NameNode healthy và rời safe mode rồi mới đọc/ghi. `verify_g02_hdfs.py` chỉ ghi vào `/trend/diagnostics`, kiểm tra SHA-256, fsck, replication và DataNode; source raw không bị sửa.

## 5. Kết luận và chuyển giao

**G02 `done` cho ViTIED trên topology Docker một host.** G03 phải upload Bronze có version và xây adapter CSV cho cả hai schema; sai lệch 3 dòng trong parser Spark là rủi ro cụ thể cần giải quyết trước khi chấp nhận Silver. Bằng chứng G02 không chứng minh huấn luyện DL4J phân tán, HA của máy chủ, hoặc dữ liệu đã được xuất sang Neo4j/Power BI.
