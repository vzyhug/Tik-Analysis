# Silver v1 — hợp đồng dữ liệu G03

Dataset hiện tại: `fb35f4b03d6fc4b3`. Version được lấy từ SHA-256 của tám checksum nguồn theo thứ tự tên file, không lấy từ thời gian chạy. Output phân tán: `/bigdata-topic/silver/datasets/<dataset_version>/` trên HDFS; kiểm tra local: `outputs/local-silver/datasets/<dataset_version>/`. Mỗi dataset gồm ba bảng Parquet sau.

| Bảng | Grain / khóa | Quy tắc |
|---|---|---|
| `videos` | Một dòng cho `video_id` duy nhất trong bộ nguồn đã đối soát | `video_id`, `author_id`, `sound_id`, `location_id`, `effects[].effect_id` đều là chuỗi. `published_at_utc` là thời gian đăng từ `createTime`. Caption, hashtag, nhạc, effect và location thiếu giữ null; mảng rỗng nguồn được giữ rỗng. Không gán Topic. |
| `observations` | Một record video nguồn: `source_file + record_index` (kèm `collection_id`) | Các count tương tác là snapshot nullable. `observed_at_utc` null vì export không có timestamp quan sát tin cậy. `published_at_utc` không thay cho thời gian quan sát. |
| `quarantine` | Một record nguồn không có Video hợp lệ: `source_file + record_index` | Giữ `reject_reason` và pointer nguồn; không đưa vào Video hoặc observation. |

Ba bảng đều lưu `source_file`, `collection_id`, `source_checksum`, `schema_version` và `dataset_version` khi phù hợp. `collection_id=PACKn` nhận diện export, chưa chứng minh các PACK là tám lần quan sát độc lập. Timestamp lưu UTC; lịch tuần sẽ tính theo `Asia/Ho_Chi_Minh` trong Gold. Thiếu count không chuyển thành 0, thiếu boolean không chuyển thành false. Dữ liệu hiện tại chưa có Video trùng ID; job dừng nếu manifest báo duplicate/conflict để tránh chọn bản cuối âm thầm.

G03 ghi staging trước, đọc lại count rồi rename thư mục version để công bố. Nếu version đã tồn tại, job chỉ đọc lại và đối soát count; không ghi đè. Bằng chứng so sánh schema/nội dung local và HDFS ở `outputs/quality/g03_validation.json`.
