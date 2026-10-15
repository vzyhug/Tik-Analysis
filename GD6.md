# Báo cáo G06 — Gold ViTIED

**Trạng thái: `done` cho dữ liệu có bằng chứng của source version hiện tại.** Topic/Sound-specific tables vẫn rỗng theo giới hạn nhãn/ID nguồn, không được xem là số 0 của mức độ phổ biến.

## Đã chuẩn bị

- Thêm Spark job [06_vitied_gold.py](jobs/spark/06_vitied_gold.py), đọc đúng Silver release đã qua G03 verification và kiểm tra G04 graph export cùng dataset version.
- Thiết kế Gold publication bất biến theo `publication_id`, ghi staging trên HDFS, đọc lại Parquet rồi atomic rename. Từ chối nếu publication/staging đã tồn tại.
- Các bảng dự kiến:
  - `post_period`: ngày đăng theo `Asia/Ho_Chi_Minh` × `media_type`, đếm Post duy nhất.
  - `hashtag_period`: ngày đăng × hashtag lấy từ Observation sớm nhất của Post; không union hashtag từ snapshot tương lai.
  - `hashtag_pair_period`: ngày đăng × cặp hashtag không thứ tự lấy từ Observation sớm nhất; đếm Post duy nhất hỗ trợ quan hệ.
  - `engagement_observation`: grain Post × `observed_at_utc`, giữ null metric và không xem snapshot là lượng tương tác phát sinh trong tuần đăng.
  - `coverage`: ngày đăng × media type; Post count, độ phủ caption/hashtag ở Observation đầu, số Observation và độ phủ từng metric.
  - `topic_period`, `topic_hashtag`, `topic_sound`: schema được giữ nhưng chưa có dòng vì chưa có nhãn Topic cấp Post được duyệt và Sound ID xác thực.
- Job kiểm tra grain Post/Observation/Hashtag, đối chiếu tập Post ID và cạnh Post–Hashtag của graph export với Silver, so count với G03/G04, yêu cầu task witness từ cả hai Spark worker, read-back count và lưu manifest provenance sau khi publish.
- `python -m py_compile jobs/spark/06_vitied_gold.py` chạy đạt.

## Bằng chứng runtime

- Spark Standalone application `app-20261014161609-0003` chạy ở client mode; executor được cấp cho `spark-worker-1` và `spark-worker-2`, manifest ghi nhận task witness trên cả hai.
- Publication HDFS: `/trend/vitied/gold/vitied-gold-v1-20261014`.
- Gold count/read-back khớp: `post_period` 613; `hashtag_period` 29.075; `hashtag_pair_period` 108.064; `engagement_observation` 34.750; `coverage` 613; các bảng Topic/Sound 0 dòng có chủ đích.
- Tổng Post trong các ngày/kind = 9.114; Silver có 34.750 Observation; graph có 9.114 Post/40.624 cạnh Post–Hashtag. Job so sánh chính xác Post ID và cạnh Post–Hashtag graph export với Silver: bằng nhau.
- HDFS fsck sau publish: `HEALTHY`, 3 DataNode, 17 block được kiểm tra, replication trung bình 2.0, 0 block thiếu/corrupt/under-replicated; log tại [g06_hdfs_fsck.txt](outputs/quality/g06_hdfs_fsck.txt).
- Manifest lưu tại [g06_gold_vitied-gold-v1-20261014.json](outputs/quality/g06_gold_vitied-gold-v1-20261014.json). Đây là bằng chứng của source version `vitied-20261013-370234d0358b`.

## Vấn đề gặp trong quá trình triển khai

Các lần chạy đầu phát hiện lỗi cột `media_type` bị trùng sau join, lỗi điều kiện `when` truyền chuỗi và task witness chỉ được lập lịch trên một worker. Job được sửa rồi chạy lại; publication được tạo đúng một lần sau khi các điều kiện trên đạt. Không có số liệu fixture được dùng thay cho bằng chứng thật.

## Lệnh chạy khi Docker hoạt động

```powershell
docker compose -f infra/compose.yaml exec -T spark-driver /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client jobs/spark/06_vitied_gold.py --silver /trend/vitied/silver/vitied-silver-v3/vitied-20261013-370234d0358b --publication-id vitied-gold-v1-20261014
```

Lệnh trên đã chạy cho publication hiện tại. Không chạy lại cùng `publication_id` vì G06 bảo vệ release bất biến; lần phát hành mới cần ID mới. G07 PostgreSQL/Power BI là gate tiếp theo, chưa triển khai trong G06.
