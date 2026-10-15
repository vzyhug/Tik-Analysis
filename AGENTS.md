# Hướng dẫn coding agent — ViTIED

Áp dụng cho toàn repository “Phân tích xu hướng chủ đề trên mạng xã hội”. Chỉ dẫn mới nhất của người dùng ưu tiên hơn file này. Đọc [README.md](README.md) và [reports/progress.md](reports/progress.md) trước khi thay đổi pipeline.

## Phạm vi và trạng thái

- ViTIED là nguồn dữ liệu duy nhất của project. Hai CSV gốc ở `data/raw/vitied/`, manifest ở `data/raw/manifests/vitied_source_20261013.json`; G01–G04 đã đạt gate cho đúng source version này theo bằng chứng có sẵn. G05 đã train DL4J phân tán nhưng còn chờ nhãn Topic cấp Post được rà soát; G06 đã đối soát; G07 dashboard đang chờ kiểm tra trực quan; G08–G09 chưa triển khai.
- Bài `/video/` và `/photo/` đều là `Post`, phân biệt bằng `media_type`. Một Post có nhiều Observation theo `observed_at_utc`. `topic` CSV là `creator_category` của tác giả, **không phải** nhãn chủ đề của Post.
- HDFS Bronze/Silver/Gold, Spark Standalone, Neo4j, DL4J và web dashboard là kiến trúc đang dùng. Neo4j là DB phục vụ web duy nhất: Post/Observation/Hashtag/Sound/Topic và quan hệ; API dashboard chỉ phát truy vấn đọc. Neo4j Community không có role read-only thực: tài khoản ứng dụng vẫn có quyền admin; bind web loopback và chỉ gửi query đọc là biện pháp giới hạn hiện tại. Docker là cụm một host: 1 NameNode + 3 DataNode (replication 2), 1 Spark Master + 2 Worker và driver riêng. G05 dùng hai JVM DL4J CPU trong hai container worker, đồng bộ trọng số bằng coordinator ở driver; đây không phải Spark executor task train.
- NiFi/Kafka replay là phần mở rộng sau batch. GDS và API chỉ triển khai khi cần. Không dùng framework ML khác thay DL4J nếu người dùng chưa đổi quyết định.

## Gate G01–G09

| Gate | Đầu ra cần kiểm tra |
|---|---|
| G01 | Kiểm kê raw, checksum, schema, canonical ID, duplicate/conflict/quarantine |
| G02 | HDFS read/write/replica/restart; Spark worker registration và task trên cả hai worker |
| G03 | Bronze bất biến; Silver Post/Observation/hashtag/category; đối soát raw và local, Parquet read-back |
| G04 | Graph Post/Observation/Hashtag/Sound/Topic theo bằng chứng, constraint, loader idempotent, count và projection |
| G05 | Split chống leakage, autoencoder DL4J 64 chiều trên caption-only features hai worker, baseline/clustering và rà soát Post; classifier chỉ khi có nhãn đã rà soát |
| G06 | Gold theo kỳ và quan hệ, distinct Post, window/version/provenance, đối soát graph |
| G07 | Web dashboard đọc Neo4j trực tiếp; ưu tiên tín hiệu hướng tới tương lai, kiểm định cuốn chiếu theo thời gian, baseline, source freshness và giới hạn leakage; không gọi cluster là Topic hay forecast stale sample là xu hướng hiện tại |
| G08 | NiFi/Kafka replay nếu chọn: timestamp nguồn, checkpoint, lỗi và sink idempotent |
| G09 | End-to-end, failure demo thử riêng, benchmark cùng dataset và tài liệu tái lập |

M1 = G01–G03; M2 = G04–G07; M3 = G08–G09 nếu chọn streaming. Khi được giao một gate, hoàn thành gate đó và kiểm tra liên quan, không tự mở rộng toàn pipeline. Chỉ đánh dấu `done` khi có bằng chứng thật trong `reports/progress.md`; file tồn tại không đồng nghĩa job/model đã chạy đạt.

## Dữ liệu và Silver

- `data/raw/vitied/` bất biến đối với job. Kiểm tra SHA-256 trước intake/đọc Bronze. Không tự tạo số liệu hoặc dùng fixture như bằng chứng dữ liệu thật.
- ID TikTok là **chuỗi**. Canonical `post_id` lấy từ URL TikTok `/video/<id>` hoặc `/photo/<id>` đã kiểm tra; giữ `raw_vid_id`, URL ID, source file/row và trạng thái giải quyết. Không ép ID qua floating point. ID mâu thuẫn phải vào quarantine có pointer.
- `vid_postTime` là thời gian đăng, `vid_scrapeTime` là thời gian quan sát. Lưu UTC; calendar dùng `Asia/Ho_Chi_Minh`. Post dùng phút đăng canonical vì một số snapshot lệch giây trong cùng phút; Observation giữ timestamp đăng nguồn.
- Không thay null bằng 0/false. `vid_nshare='Share'` là missing. Caption/hashtag/âm thanh thiếu không làm mất Post. Hashtag: NFC → trim → bỏ `#` đầu → casefold, giữ dấu tiếng Việt.
- `music_id` dạng ký pháp khoa học không phục hồi bằng float và không được gộp Sound theo tên. Silver hiện có `sound_id` unresolved cho toàn bộ 34.750 Observation; G04 không tạo Sound từ ID suy đoán.
- Cùng Post × `observed_at_utc` chỉ một Observation active. Duplicate chính xác, conflict, identity ambiguity và row lỗi phải đối soát/quarantine. Silver release viết staging, đọc lại rồi publish; không ghi đè path đã phát hành.

## Graph, ML và báo cáo

- Graph mục tiêu: `Post`, `Hashtag`, `Sound`, `Topic`. `HAS_HASHTAG` từ Silver; `USES_SOUND` chỉ từ Sound ID xác thực; `HAS_TOPIC` chỉ từ nhãn Post đã rà soát hoặc prediction provisional được công bố riêng. `creator_category` không tạo Topic edge. Không tự tạo Effect/Location khi CSV không có nguồn.
- Loader Neo4j dùng parameter/batch, uniqueness constraints trước load, khóa có namespace/datasetId ViTIED, kiểm tra reload không nhân đôi. Cạnh aggregate dùng `COUNT DISTINCT post_id` trong cửa sổ đăng nửa mở theo timezone cấu hình, lưu version/provenance và không tự cộng dồn khi chạy lại.
- Chia split theo Post/author và caption trùng **trước** fit IDF/scaler. Snapshot cùng Post ở cùng split; feature hiện hành chỉ lấy caption từ Observation sớm nhất; hashtag không tham gia DL4J caption-only. Cluster là thăm dò; nhãn chưa rà soát là unknown, không mặc định negative. G05 đã xuất review sheet nhưng chưa có nhãn manual. Classifier nhiều nhãn cần nhãn kiểm tra, baseline, Micro/Macro-F1, per-topic support, threshold chọn trên validation, save/load round-trip và prediction có version. Inference không ghi đè nhãn manual.
- Mỗi bảng Gold HDFS có grain, khóa, kiểu, timezone, null policy và schema version. Đếm Post duy nhất; tổng các Topic nhiều nhãn không bằng tổng Post. View/like theo tuần đăng là snapshot quan sát, không phải lượt phát sinh trong tuần. Delta cần ít nhất hai Observation nguồn có timestamp phù hợp. Dashboard dùng graph và model artifact đã phát hành; dự báo phải so với baseline trên chronological backtest, báo độ mới nguồn và giới hạn leakage; không khởi chạy Spark/training khi tải hoặc làm mới.

## Hạ tầng, kiểm tra và cách làm việc

- Giữ Docker volumes NameNode/DataNode/Spark/Neo4j; không format NameNode, `down -v` hoặc reset database trong lệnh chạy thường lệ. Địa chỉ trong container dùng service name, từ Windows dùng cổng publish. PySpark Standalone chạy `spark-submit` client mode từ driver.
- HDFS kiểm tra bằng read-back SHA/count và fsck block/locations; Spark kiểm tra event/task trên cả hai worker và output tương đương local. Một host/container không chứng minh HA host. Benchmark giữ cùng dataset/job, core/RAM và output.
- Python: module/hàm rõ nhiệm vụ, schema Spark tường minh, type hints cho điều phối, Spark built-in khi phù hợp; tránh collect toàn bộ data cho xử lý phân tán. Java DL4J khóa phiên bản đồng bộ, chạy CPU/native smoke trước training. Secret lấy từ file/env, không commit token/password.
- Tìm file/text bằng `rg --files`/`rg`. Kiểm tra Git trước sửa, giữ thay đổi người dùng ngoài phạm vi. Cập nhật README và progress khi thay đổi entry point, schema hoặc trạng thái gate. Test hành vi/rủi ro thật; nếu thiếu Docker hoặc dữ liệu, báo giới hạn, không mô tả fixture như nghiệm thu cụm.
