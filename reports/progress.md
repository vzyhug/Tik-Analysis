# Tiến độ G01–G09

| Giai đoạn | Trạng thái | Bằng chứng và phần còn thiếu |
|---|---|---|
| G01 | done | Đã kiểm kê 8 PACK thật và đối soát `data/reference/kol.csv`: 720 dòng, 578 username, 24 danh mục; 527 có record nguồn, 475 có Video. Manifest SHA-256, schema, missing/quarantine tại `data/raw/manifests/` và `outputs/quality/`. |
| G02 | done | `outputs/quality/g02_cluster_evidence.json`: 3 DataNode, 8 block Bronze đều có 2 replica, fsck healthy, 8 SHA-256 read-back; restart giữ namespace/block. Hai Worker Spark ALIVE, smoke job xử lý trên cả hai và ghi/đọc Parquet HDFS. Neo4j profile chạy và trả lời truy vấn. |
| G03 | done | Silver `fb35f4b03d6fc4b3` trên HDFS: 4.673 Video, 4.673 observation, 52 quarantine. `g03_validation.json`: ba bảng khớp local từng dòng, không lệch; 145 Video không có caption/hashtag, `observed_at` null đúng nguồn. `g03_event_evidence.json`: job publish thử hoàn chỉnh có task trên cả hai Worker (67/70). |
| G04 | not_started | Neo4j instance đã chạy, nhưng chưa có constraints, graph tables, loader hoặc query dữ liệu thật. |
| G05 | in_progress | Có `topics_v0_draft.csv`, hướng dẫn gán nhãn và hàng chờ 96 Video từ đủ 24 danh mục tham khảo; tất cả chưa rà soát, chưa có ground truth hoặc model DL4J. |
| G06 | not_started | Chưa có Topic active, aggregate graph hoặc Gold. |
| G07 | not_started | Chưa có export/dashboard Power BI. |
| G08 | not_started | Chưa có NiFi/Kafka replay. |
| G09 | not_started | Chưa có nghiệm thu end-to-end/benchmark. |

## Việc đã chạy ngày 11/10/2026

- `python scripts/intake.py`: 4.725 record, 4.673 Video ID, 36 lỗi nguồn, 16 record profile only; SHA-256 theo PACK và schema từng file trong manifest. `kol.csv` có 578 username, 51 không có record nguồn và 52 có record nhưng không có Video.
- `python -m unittest discover -s tests -v`: 3 kiểm tra đạt; đọc JSONL, ID dài, duplicate/conflict, null/0/false, bảo vệ raw và đối soát KOL.
- Môi trường: Python 3.13.0; Docker CLI/Engine 29.1.3, Compose v5.0.0-desktop.1; Docker cấp 8 GiB RAM/16 CPU logic; Java host 25.0.1 và Maven 3.9.12. Đã chạy Hadoop 3.4.2, Spark 4.0.2/Java 17 trong image; DL4J chưa kiểm chứng.
- G02: `python scripts/upload_bronze.py`, `python scripts/capture_g02_evidence.py`; HDFS 3 DataNode/replication 2, restart không mất 8 PACK; Spark 2 Worker x 2 core/1536 MiB, driver client mode. Neo4j Community 5.26.30 khởi động, xác nhận `RETURN 1`.
- G03: `jobs/spark/01_prepare_data.py` chạy Spark Standalone và local; `jobs/spark/02_verify_data.py` xác nhận schema/nội dung tương đương, không có dòng lệch. Event log của job publish thử thành công: 67 task trên Worker 1, 70 trên Worker 2. `schema_version=silver-v1`, `dataset_version=fb35f4b03d6fc4b3`.

## Tiếp theo

Người thực hiện rà soát `data/reference/topics_v0_draft.csv` và bắt đầu điền `data/labels/review_queue.csv`; chưa coi danh mục KOL là nhãn. G04 dùng Silver đã xuất. Docker chỉ có 8 GiB RAM nên worker đặt 1536 MiB thay vì mức khởi điểm 2 GiB; theo dõi RAM khi chạy job cùng Neo4j.
