# Báo cáo G03 — ViTIED Bronze → Silver

**Ngày nghiệm thu:** 13/10/2026. **Trạng thái:** `done` cho source version `vitied-20261013-370234d0358b`, Silver active `vitied-silver-v3`. G01 và G02 đã nghiệm thu riêng trong [GD1.md](GD1.md) và [GD2.md).

## Đầu vào và cách xử lý

- [Manifest nguồn](data/raw/manifests/vitied_source_20261013.json) định danh hai CSV raw bất biến bằng SHA-256. [Script Bronze](scripts/upload_vitied_bronze.py) tải vào `/trend/vitied/bronze/vitied-20261013-370234d0358b/`; nếu file đã tồn tại chỉ xác minh checksum, không ghi đè. [Bằng chứng Bronze](outputs/quality/g03_bronze.json) xác nhận hai checksum khớp.
- Spark `.csv(multiLine=true)` ở thử nghiệm G02 chỉ đọc 29.633/29.636 dòng training. [Job G03](jobs/spark/03_vitied_silver.py) đọc chính byte CSV từ HDFS trong task Spark bằng Python `csv.DictReader` để giữ record có quoted newline; sau đó repartition 32 và chuẩn hóa trên hai worker. Không dùng bản parser thiếu dòng để xuất Silver.
- Canonical `post_id`/`media_type` lấy từ URL TikTok đã kiểm tra, giữ `raw_vid_id`. `published_at_utc` gốc giữ trong Observation; Post lấy phút đăng UTC. `observed_at_utc` là thời gian quan sát nguồn. `topic` nguồn chỉ thành creator category; không gán Topic bài. Hashtag NFC → bỏ `#` → casefold. Metric K/M được parse; `Share` và thiếu là null. `music_id` dạng khoa học không bị đoán thành ID Sound.
- Job kiểm tra xung đột raw ID ↔ URL ID, author/media type/phút đăng, observation trùng hoặc khác fingerprint; row lỗi/bản sao vào bảng quarantine với source pointer. Release viết vào staging, kiểm count/read-back rồi rename sang đường dẫn cuối. Không ghi đè release đã có.

## Đối soát nguồn → Silver

| Chỉ tiêu | Kết quả |
|---|---:|
| Raw training / streaming | 29.636 / 5.114 |
| Raw tổng | 34.750 |
| Observation accepted | 34.750 |
| Duplicate / conflicting observation | 0 / 0 |
| Identity conflict / quarantine | 0 / 0 |
| Post canonical | 9.114 (8.731 video, 383 photo) |
| Post–Hashtag distinct | 40.624 |
| Username–creator category distinct | 402 |
| Placeholder `vid_nshare='Share'` lưu null | 848 |
| Observation có Sound ID xác thực | 0; 34.750 giữ raw music ID nhưng `sound_id` null |

Phép đối soát **34.750 raw = 34.750 accepted + 0 quarantine** và count read-back cả năm bảng Parquet ở [g03_silver.json](outputs/quality/g03_silver.json). [Bảng ID G01](outputs/quality/vitied_id_resolution.csv) so với Post Silver: **0 thiếu, 0 thừa**, gồm media type, author, phút đăng và số snapshot. Khóa Observation tạo độc lập bằng Python stdlib CSV từ raw so với Silver: **0 thiếu, 0 thừa** ở [g03_verify.json](outputs/quality/g03_verify.json). `g03_expected_observation_keys.csv` là artifact đối chiếu local, không phải nguồn huấn luyện mới.

## Cụm và kiểm tra

- App Spark G03 `app-20261013163900-0005` chạy `spark://spark-master:7077`; task chuẩn hóa có trên `spark-worker-1` và `spark-worker-2` ([bằng chứng](outputs/quality/g03_silver.json)). Read-back Parquet: Post 9.114, Observation 34.750, Post–Hashtag 40.624, creator category 402, quarantine 0.
- `python scripts/verify_g03_hdfs.py`: [fsck Bronze](outputs/quality/g03_hdfs_fsck_bronze.txt) có 2 block; [fsck Silver](outputs/quality/g03_hdfs_fsck_silver.txt) có 12 block; 3 DataNode, replication trung bình 2, 0 missing block ([tóm tắt](outputs/quality/g03_hdfs.json)).
- `spark-submit --master local[1] tests/test_g03_silver.py`: 2 test adapter đạt, kiểm quoted multiline, URL video/photo, `Share` null, K/M và NFC hashtag. `python -m unittest discover -s tests -p test_vitied_*.py -v`: 4 test G01 hồi quy đạt.

## Hợp đồng Silver và giới hạn

Release active: `/trend/vitied/silver/vitied-silver-v3/vitied-20261013-370234d0358b/`, gồm `posts` (một Post), `observations` (một Post × thời điểm quan sát UTC), `post_hashtags` (một Post × hashtag), `creator_categories` (một username × category), `quarantine` (một source row lỗi/bản sao). `v1/v2` là output thử nghiệm ViTIED được giữ để audit, **không dùng downstream**. CSV raw ViTIED được giữ nguyên.

| Bảng | Grain/khóa | Kiểu và null policy chính |
|---|---|---|
| `posts` | `post_id` duy nhất | ID/`media_type`/`author_username` chuỗi; `published_minute_utc` timestamp UTC độ phân giải phút; `observation_count` long; `dataset_version`, `schema_version` chuỗi |
| `observations` | `post_id` × `observed_at_utc` | `source_file` chuỗi, `source_row` long; raw/canonical ID chuỗi; `published_at_utc` timestamp UTC gốc; caption/hashtag/music/category nguồn giữ raw; view/like/comment/share/save long nullable, null khác 0; `sound_id` nullable; `dataset_version`, `schema_version` chuỗi |
| `post_hashtags` | `post_id` × `tag_key` | Hai khóa chuỗi non-null, hashtag NFC/casefold, `dataset_version` chuỗi; bài không có hashtag không sinh hàng |
| `creator_categories` | `author_username` × `creator_category` | Hai khóa chuỗi non-null, provenance từ `topic` nguồn, `dataset_version` chuỗi; unknown không sinh hàng |
| `quarantine` | `source_file` × `source_row` | Source pointer, raw/canonical ID và `error` chuỗi; một dòng có thể mang danh sách lý do; source version hiện tại rỗng |

Giờ phân tích tuần/ngày ở tầng Gold phải chuyển từ UTC sang `Asia/Ho_Chi_Minh`; Observation không được hiểu là tương tác mới phát sinh tại tuần đăng. Với quan hệ nhiều dòng, đếm bài phải dùng `COUNT DISTINCT post_id`.

G03 đã đối chiếu grain và dữ liệu trên source hiện tại. Fixture kiểm tra parser, nhưng chưa có bộ fixture Spark tích hợp riêng cho mọi nhánh conflict/quarantine; khi nguồn thay đổi phải chạy lại G01/G03 và duyệt quarantine. Sound ID hiện không đủ chính xác để tạo node/cạnh Sound ở G04; chỉ xây Post–Hashtag từ bằng chứng hiện có. G03 chưa có nhãn Topic cấp bài, model DL4J, graph, Gold hoặc Power BI ViTIED.

## Cách chạy lại

Các lệnh và đường dẫn version hiện hành ở [README](README.md#chạy-lại-g03-cho-source-version-hiện-tại). Khi phát hành Silver mới, chọn path mới cùng schema version, đối chiếu bằng `jobs/spark/03_verify_vitied_silver.py`, rồi cập nhật đường dẫn active trong script fsck/tài liệu. Không ghi đè path release đã tồn tại.
