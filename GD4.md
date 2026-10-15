# Báo cáo G04 — đồ thị ViTIED

**Ngày nghiệm thu:** 14/10/2026. **Trạng thái:** `done` cho các quan hệ có bằng chứng trong source version `vitied-20261013-370234d0358b`. Sound và Topic đang chờ dữ liệu/nhãn hợp lệ.

## Đầu vào và hợp đồng đồ thị

G04 đọc Silver active `/trend/vitied/silver/vitied-silver-v3/vitied-20261013-370234d0358b`, chỉ khi [G03 Silver](outputs/quality/g03_silver.json) và [đối chiếu G03](outputs/quality/g03_verify.json) đều đạt. [Spark export](jobs/spark/04_export_vitied_graph.py) tạo 5 bảng JSONL tại `outputs/g04/vitied-20261013-370234d0358b/` qua staging, kiểm tra read-back, SHA-256 từng part và task trên hai worker. [Manifest export](outputs/quality/g04_graph_export.json) gắn Silver path, source/schema/graph version và count.

Neo4j dùng label mục tiêu `Post`, `Hashtag`, `Sound`, `Topic` với uniqueness constraint cho `postKey`, `tagKey`, `soundKey`, `topicKey`. Khóa có namespace `vitied:` và node có `datasetId='vitied'`. `Post` phân biệt `mediaType=video/photo`; thời gian đăng lưu UTC theo độ phân giải phút từ Silver. Loader không tạo node Observation/Author hay Topic từ `creator_category`. Quan hệ gốc hiện có `Post` → `HAS_HASHTAG` → `Hashtag`, một cạnh cho một cặp Post × tag. `USES_SOUND` chỉ được sinh từ `sound_id` chính xác; `HAS_TOPIC` đợi nhãn Topic cấp Post có provenance. Không suy diễn âm thanh từ tên hoặc ID ký pháp khoa học.

## Kết quả nạp và đối soát

| Chỉ tiêu | Export | Sau nạp | Sau nạp lại |
|---|---:|---:|---:|
| Post | 9.114 | 9.114 | 9.114 |
| Hashtag | 10.549 | 10.549 | 10.549 |
| HAS_HASHTAG | 40.624 | 40.624 | 40.624 |
| Sound / USES_SOUND | 0 / 0 | 0 / 0 | 0 / 0 |
| Topic / HAS_TOPIC | 0 / 0 | 0 / 0 | 0 / 0 |

[Báo cáo Neo4j](outputs/quality/g04_graph_load.json) ghi `passed=true`, 4 constraint, checksum đầu vào đạt và count lần đầu bằng lần chạy lại. Các thao tác `MERGE` theo khóa/cặp node; loader chỉ ghi các thuộc tính Post, Hashtag, Sound và hai quan hệ nguồn. Khi G05 bổ sung nhãn manual, chạy lại G04 không ghi đè `HAS_TOPIC`. Loader dừng nếu Post ViTIED đã có dataset version khác; migration phiên bản cần thực hiện có chủ đích.

Projection `Post`–`HAS_HASHTAG`–`Hashtag` bỏ hướng, toàn khoảng thời gian, gồm 1.571 component; component lớn nhất có 16.489 node, trong đó 7.286 Post; 1.176 Post không có hashtag. Đây là phép đo theo cạnh hashtag nguồn, không phải bằng chứng cộng đồng chủ đề hoặc lan truyền. Các Post không hashtag vẫn được giữ.

## Cách chạy lại

Từ gốc repository, với Docker stack G02/G03 đang chạy:

```powershell
docker compose -f infra/compose.yaml exec -T spark-driver /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client jobs/spark/04_export_vitied_graph.py --silver /trend/vitied/silver/vitied-silver-v3/vitied-20261013-370234d0358b --output /workspace/outputs/g04/vitied-20261013-370234d0358b
python graph/load_vitied.py --export outputs/g04/vitied-20261013-370234d0358b
python -m unittest tests/test_vitied_graph.py -v
```

Export release là bất biến: nếu thư mục đã tồn tại, dùng manifest hiện có cho lệnh loader; không ghi đè. Loader lấy auth từ `config/secrets/neo4j_auth.txt` (không commit) hoặc `--auth-file`, endpoint mặc định `http://127.0.0.1:7474` hoặc `NEO4J_HTTP_URL`. Lệnh loader chạy hai lượt để kiểm tra idempotence; báo cáo mới ghi ở `outputs/quality/g04_graph_load.json`.

## Kiểm tra và giới hạn

- Spark app `app-20261014082429-0001` chạy trên `spark://spark-master:7077`, witness cả `spark-worker-1` và `spark-worker-2`; 5 bảng đọc lại đúng count. Loader kiểm tra SHA-256, grain, khóa đích, ID Sound và quan hệ mồ côi trước khi ghi. Ba unit test kiểm tra projection, trùng/mồ côi cạnh và ID Sound không hợp lệ. `python -m py_compile` cho hai script đạt.
- Cả 34.750 Observation có `sound_id=null` do `music_id` nguồn không khôi phục chính xác; vì vậy `Sound` và `USES_SOUND` bằng 0 theo dữ liệu, không phải lỗi bỏ sót. Chưa có nhãn Topic cấp bài được rà soát; `Topic` và `HAS_TOPIC` bằng 0. Bốn label mục tiêu đã có constraint, nhưng chỉ hai label có node ở release này.
- Projection hiện không xét độ tương đồng văn bản, nhãn Topic hoặc cửa sổ thời gian. G05 xử lý nhãn/mô hình; G06 xử lý tổng hợp theo cửa sổ và Gold. Không dùng count component làm thước đo chất lượng model.

## Bổ sung Observation v2 cho dashboard

Ngày 16/10 tạo graph export v2 bất biến từ cùng Silver source version, bổ sung bảng `observations` với 34.750 hàng Post × `observed_at_utc`; mỗi hàng giữ metric snapshot, caption và source pointer. Spark task chạy trên `spark-worker-1` và `spark-worker-2`, JSON đọc lại đúng count. Loader tạo uniqueness constraint `Observation.observationKey`, `HAS_OBSERVATION`, nạp và reload cùng 34.750 Observation/relationship; [report hiện hành](outputs/quality/g04_graph_load.json) ghi `passed=true`. Đây là mở rộng additive, không xóa graph v1, không tạo Sound/Topic giả.
