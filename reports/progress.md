# Trạng thái pipeline ViTIED

Ngày cập nhật: 16/10/2026. Source version: manifest `vitied_source_20261013.json`. Đây là trang theo dõi duy nhất cho G01–G09; số liệu chỉ được ghi là đạt khi có report tương ứng trong `outputs/quality/`.

## Tổng quan gate

| Gate | Trạng thái | Bằng chứng / kết quả | Còn thiếu |
|---|---|---|---|
| G01 — Intake | `done` cho source version hiện tại | [Source audit](../outputs/quality/vitied_source_audit.json), [G01](../outputs/quality/vitied_g01.json), [manifest](../data/raw/manifests/vitied_source_20261013.json), [ID resolution](../outputs/quality/vitied_id_resolution.csv). 34.750 Observation, 9.114 Post (8.731 video, 383 photo), 384 creator; không có quarantine/conflict ở bản hiện tại. | Chạy lại audit nếu source/checksum/schema thay đổi. |
| G02 — HDFS/Spark nền | `done` | [HDFS](../outputs/quality/g02_hdfs.json), [restart](../outputs/quality/g02_hdfs_restart.json), [fsck](../outputs/quality/g02_hdfs_fsck.txt), [Spark hai worker](../outputs/quality/g02_spark_binary.json). SHA/read-back, 3 DataNode replication 2, Spark task trên cả hai worker. | Không có. Cụm là một host, không phải HA host. |
| G03 — Bronze/Silver | `done` cho source version hiện tại | [Bronze](../outputs/quality/g03_bronze.json), [Silver](../outputs/quality/g03_silver.json), [đối soát](../outputs/quality/g03_verify.json), [fsck](../outputs/quality/g03_hdfs.json). 34.750 accepted Observation, 9.114 Post; raw/local/read-back khớp; active Silver `vitied-silver-v3`. | Source/schema mới cần release mới và đối soát lại. |
| G04 — Neo4j graph | `done` cho quan hệ có bằng chứng | [Spark export](../outputs/quality/g04_graph_export.json), [Neo4j load](../outputs/quality/g04_graph_load.json). 9.114 Post, 34.750 Observation, 10.549 Hashtag, 40.624 `HAS_HASHTAG`; reload idempotent. | Sound ID chưa xác thực; Topic cần nhãn Post đã duyệt. |
| G05 — DL4J | `in_progress` | [Feature/split caption-only](../outputs/quality/g05_features_vitied-g05-caption-idf512-v1.json), [train report](../outputs/quality/g05_train_vitied-g05-captiononly-ae64-live-20261016.json), [worker console](../outputs/quality/g05_train_vitied-g05-captiononly-ae64-live-20261016.console.txt), [DL4J/SVD evaluation](../outputs/quality/g05_evaluation_vitied-g05-captiononly-ae64-live-20261016.json). 64D autoencoder train trên 2 worker, 3 vòng, 47 batch/worker/vòng; 8.254 vector, 860 abstain. | [Phiếu 80 Post](../data/labels/vitied_g05_review_mixed.csv) chưa có nhãn manual. Chưa thể nghiệm thu Topic/classifier hoặc so F1. Cluster chỉ để rà soát. |
| G06 — Gold | `done` cho dữ liệu hiện có | [Gold manifest](../outputs/quality/g06_gold_vitied-gold-v1-20261014.json), [fsck](../outputs/quality/g06_hdfs_fsck.txt). 9.114 Post, 34.750 Observation; read-back, Post ID và quan hệ hashtag đối soát. | Topic tables chưa có dòng khi thiếu nhãn duyệt; Sound chờ ID xác thực. |
| G07 — Web dashboard | `in_progress` | [Verifier](../outputs/quality/g07_web_dashboard.json), 40 checks đạt. Dashboard đọc Neo4j trực tiếp; 6 mục báo cáo, mạng hashtag, trend/cohort. | Kiểm tra trực quan desktop/màn hẹp khi có browser UI. Nguồn kết thúc 02/07/2025 nên dự báo là hồi cứu. |
| G08 — Streaming replay | `not_started` (tùy chọn) | Chưa triển khai NiFi/Kafka. | Chỉ làm khi mở rộng phạm vi; cần timestamp nguồn, checkpoint, lỗi, idempotent sink. |
| G09 — End-to-end | `not_started` | Chưa có nghiệm thu end-to-end/failure demo/benchmark tổng. | Chạy sau khi chốt các gate và cùng dataset/tài nguyên/output. |

## Quy ước và giới hạn quan trọng

- ViTIED là nguồn duy nhất. CSV gốc được giữ ngoài Git, kiểm tra theo manifest SHA-256.
- `topic` là creator category, không phải nhãn Topic bài. Không dùng làm ground truth hoặc tạo `HAS_TOPIC`.
- `music_id` ở ký pháp khoa học không khôi phục bằng float/đoán tên; hiện toàn bộ 34.750 Observation chưa có Sound ID xác thực.
- Graph/dashboard không tự tạo Effect hoặc Location vì nguồn không có các trường này.
- G05 giữ author/caption-duplicate split và fit IDF trên train; caption-only bỏ hashtag. Nhãn chưa duyệt là unknown, prediction không ghi đè nhãn manual.
- Hashtag phổ thông được giữ trong thống kê nhưng loại khỏi scoring khi vượt 5% Post. Link scoring cần tối thiểu 3 Post và 2 creator; kỳ hiện tại có 10.549 tag, 62.067 cặp ứng viên, 3.451 cặp được chấm; ArticleRank và Closeness đều dẫn đầu bởi `#skincare`.
- Dashboard chỉ mô tả mẫu ViTIED. Baseline giữ tỷ trọng gần nhất MAE 2,91 điểm %, thấp hơn trend MAE 3,06 điểm % trên 7 cửa sổ thăm dò; dữ liệu kết thúc 02/07/2025 nên không phải dự báo xu hướng TikTok hiện tại.

## Lệnh kiểm tra nhanh

```powershell
python scripts/verify_g07_web_dashboard.py
python -m unittest discover -s tests -v
python -m compileall -q scripts graph apps/web_dashboard
node --check apps/web_dashboard/static/app.js
docker compose --profile graph -f infra/compose.yaml -f infra/compose.web.yaml ps -a
```

Không ghi đè release hiện hành. Mỗi lần xuất Silver/Gold/model dùng version hoặc publication ID mới. Giữ nguyên Docker volumes; không dùng `down -v` hoặc format NameNode trong vận hành bình thường.
