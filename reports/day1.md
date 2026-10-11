# Ngày 1 — Hạ tầng và dữ liệu (11/10/2026)

## Kết quả đã kiểm chứng

| Phần | Kết quả | Bằng chứng |
|---|---|---|
| G01 intake | 8 PACK; 4.725 record = 4.673 Video + 36 lỗi nguồn + 16 profile only; `kol.csv` có 578 username/24 danh mục | `data/raw/manifests/apify_g01.json`, `outputs/quality/g01_inventory.md` |
| HDFS | 1 NameNode + 3 DataNode, volume độc lập; 8 file Bronze SHA-256 đọc lại khớp; 8 block có `Live_repl=2`, fsck healthy | `outputs/quality/g02_bronze_readback.json`, `g02_cluster_evidence.json`, `g02_fsck.txt` |
| Restart | Restart NameNode và 3 DataNode, namespace giữ nguyên; `fsck` lại healthy, read-back 8 checksum khớp | Lệnh restart trong phiên và `g02_cluster_evidence.json` sau restart |
| Spark | 1 Master + 2 Worker, mỗi Worker 2 core/1536 MiB; driver client mode; smoke đọc 8 file Bronze và ghi/đọc Parquet HDFS | `outputs/quality/g02_spark_smoke.json` |
| G03 Silver | `silver-v1`, dataset `fb35f4b03d6fc4b3`: 4.673 videos, 4.673 observations, 52 quarantine | `outputs/quality/g03_normalize.json`, `g03_validation.json` |
| So sánh local | Ba bảng Parquet có schema giống và `exceptAll` hai chiều đều 0 dòng lệch; 145 Video thiếu caption + hashtag, `observed_at` null cho mọi observation | `outputs/quality/g03_validation.json` |
| Task G03 | Job publish thử thành công trên cùng input/HDFS có 67 task thành công trên Worker 1 và 70 trên Worker 2 | `outputs/quality/g03_event_evidence.json`, app `app-20261011151642-0008` |
| Neo4j | Community 5.26.30 đã khởi động với volume, truy vấn `RETURN 1 AS ok` trả 1 | Service `neo4j` profile `graph`; chưa nạp đồ thị |

Job chuẩn hóa ban đầu ghi và đối soát staging thành công nhưng bước chuyển thư mục vào `datasets/` thất bại vì thiếu thư mục cha. Đã tạo thư mục cha, publish staging đã kiểm tra, sửa job và chạy lại đầy đủ trên vùng thử riêng; lần publish thử thành công. Dataset chính sau đó được đọc lại và đối soát local/HDFS.

## Tài nguyên và giới hạn

- Máy Windows có 16 GiB RAM/16 CPU logic; Docker Engine được cấp khoảng 8 GiB RAM. Worker dùng 1536 MiB thay cho mức tham khảo 2 GiB để giữ chỗ cho HDFS, driver và Neo4j.
- Demo phân tán chạy nhiều container trên cùng một host; không chứng minh chịu lỗi mất host. NameNode đơn vẫn là điểm lỗi đơn. Replication không phải backup.
- `observed_at` không có trong export và giữ null. Các bản ghi không có caption/hashtag vẫn được giữ trong Video Silver, không tự gán Topic.
- Neo4j chưa nạp node/cạnh. Taxonomy còn nháp, hàng chờ 96 Video đều `unreviewed`, chưa có dữ liệu huấn luyện có giám sát.

## Việc cần người thực hiện

1. Duyệt/sửa `data/reference/topics_v0_draft.csv` theo hướng dẫn `labeling_guide_v0.md`.
2. Mở `data/labels/review_queue.csv` bằng công cụ giữ ID dạng chuỗi, rà soát caption/hashtag và điền nhãn có bằng chứng. Trên bản clone mới, chạy `python scripts/prepare_label_review.py` để tạo queue; queue chứa caption nên không commit. Không đổi `unreviewed` thành negative khi chưa xem.

Ngày 2 có thể bắt đầu G04 từ Silver, đồng thời tiếp tục gán nhãn và chuẩn bị split G05.
