# Tiến độ G01–G09

| Giai đoạn | Trạng thái | Bằng chứng và phần còn thiếu |
|---|---|---|
| G01 | in_progress | Đã kiểm kê 8 PACK thật bằng `python scripts/intake.py`; manifest, báo cáo và pointer quarantine trong `data/raw/manifests/`, `outputs/quality/`, `data/quarantine/`. Chưa có `data/reference/kol.csv` để đối soát danh sách 578 KOL đầu vào. |
| G02 | not_started | Docker CLI/Compose có sẵn; Docker daemon chưa chạy. Chưa có cluster hoặc bằng chứng replica/read-back. |
| G03 | not_started | Chưa có job Spark, Silver, HDFS Bronze hoặc bằng chứng task hai worker. |
| G04 | not_started | Chưa có Neo4j hoặc graph loader. |
| G05 | not_started | Chưa có nhãn video kiểm tra hoặc model DL4J. |
| G06 | not_started | Chưa có Topic active, aggregate graph hoặc Gold. |
| G07 | not_started | Chưa có export/dashboard Power BI. |
| G08 | not_started | Chưa có NiFi/Kafka replay. |
| G09 | not_started | Chưa có nghiệm thu end-to-end/benchmark. |

## Việc đã chạy ngày 11/10/2026

- `python scripts/intake.py`: 4.725 record, 4.673 Video ID, 36 lỗi nguồn, 16 record profile only; SHA-256 theo PACK và schema từng file trong manifest.
- `python -m unittest discover -s tests -v`: kiểm tra đọc JSONL, ID dài, duplicate/conflict, null policy và bảo vệ raw (chạy lại sau khi sửa test).
- Môi trường: Python 3.13.0; Docker CLI 29.1.3/Compose v5.0.0-desktop.1; Java 25.0.1; Maven 3.9.12. Docker daemon không kết nối được, chưa xác nhận runtime Spark/Hadoop/DL4J.

## Tiếp theo

Đối soát `kol.csv` khi có danh sách đầu vào, rồi triển khai G02 và kiểm chứng HDFS/Spark thực tế. G01 chỉ đánh dấu `done` khi danh sách KOL, manifest và mapping được đối soát đầy đủ.
