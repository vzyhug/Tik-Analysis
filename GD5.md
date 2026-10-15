# Báo cáo G05 — DL4J phân tán và rà soát chủ đề ViTIED

**Ngày chạy:** 14/10/2026. **Trạng thái:** `in_progress`. Đã triển khai, chạy và kiểm chứng huấn luyện autoencoder DL4J trên hai JVM worker; chưa nghiệm thu Topic cấp Post vì chưa có nhãn người rà soát. Không nạp cluster thành `HAS_TOPIC`.

## Nguồn, grain và split

[Spark export](jobs/spark/05_export_vitied_posts.py) đọc Silver active `vitied-silver-v3/vitied-20261013-370234d0358b`, chọn đúng **Observation sớm nhất mỗi Post** theo `observed_at_utc`, `source_file`, `source_row`. Caption và hashtag lấy cùng Observation; không dùng bảng hashtag hợp nhất qua các snapshot để train. Export có 9.114 Post, read-back 9.114 và task trên `spark-worker-1`, `spark-worker-2` ([bằng chứng](outputs/quality/g05_content_export.json)). Trong snapshot đầu, 781 Post thiếu caption, 1.179 thiếu hashtag và 325 thiếu cả hai.

[Job feature](scripts/prepare_g05_features.py) split theo tác giả và gom tác giả có caption dài trùng nguyên văn vào cùng split. Mọi snapshot của một Post nằm chung một split. Hashing 512 chiều giữ từ hiếm; IDF chỉ fit trên train, sau đó chuẩn hóa L2. Phiên bản feature active là `vitied-g05-hashidf512-v3`; `v1` có 355 vector 0 do giới hạn từ vựng, `v2` còn một caption trùng xuyên split, nên hai bản đó là bằng chứng thử nghiệm, **không dùng train chính**. [Manifest feature](outputs/quality/g05_features.json) có checksum và source version.

| Grain | Train | Validation | Test | Tổng |
|---|---:|---:|---:|---:|
| Post trước kiểm tra nội dung | 6.597 | 1.095 | 1.422 | 9.114 |
| Post có vector sử dụng | 6.351 | 1.070 | 1.341 | 8.762 |
| Tác giả | 276 | 48 | 60 | 384 |

**352 Post abstain**: 325 thiếu caption/hashtag ở snapshot đầu và 27 có nội dung nhưng tokenizer không tạo token. Kiểm tra manifest: Post duy nhất 9.114, 0 tác giả chéo split, 0 nhóm caption dài trùng nguyên văn chéo split. Đây là kiểm tra exact duplicate; chưa có gate near-duplicate ngữ nghĩa. Nội dung được quan sát tại thời điểm scrape, chưa chứng minh đúng nội dung ngay lúc đăng.

## Huấn luyện thực trên hai worker

[Autoencoder Java](apps/dl4j/src/main/java/vn/bigdata/topic/DistributedAutoencoder.java) có kiến trúc **512–128–64–128–512**, loss MSE. Maven dùng DL4J/ND4J `1.0.0-M2.1`; bỏ dependency ảnh/video không cần cho văn bản. ND4J CPU smoke đã khởi tạo và lưu mô hình 148.288 tham số. [Coordinator](scripts/train_g05_distributed.py) cho hai JVM DL4J chạy đồng thời trong container `spark-worker-1` và `spark-worker-2`; mỗi JVM train shard riêng, ghi model/host/số mẫu/độ thay đổi tham số. JVM trong `spark-driver` gộp tham số theo trọng số **số Post**, lưu checkpoint mỗi vòng, đánh giá validation và chọn checkpoint tốt nhất. Đây là huấn luyện phân tán giữa hai container worker với điều phối riêng; **các lượt train không phải Spark executor task**. Spark được dùng để chuẩn bị đầu vào từ HDFS.

Run đầu dùng SGD 0,05 trong ba vòng: hai worker xử lý 3.176 và 3.175 Post, cả hai cập nhật tham số; MSE test 0,002180, kém cả dự đoán 0. Run này được giữ để audit, không chọn. Run `vitied-g05-ae64-v2` dùng Adam 0,003 và ba vòng; [train report](outputs/quality/g05_train_v2.json) ghi validation giảm 0,002233 → 0,001728, test 0,001734. Save/load `best.zip` rồi tính lại validation cho kết quả **chênh lệch 0** ([round-trip](outputs/quality/g05_roundtrip.json)).

Run tiếp từ checkpoint v2 thêm tám vòng thành `vitied-g05-ae64-v3`; [train report](outputs/quality/g05_train.json) ghi validation 0,001728 → 0,001578, test 0,001565. Validation gần như đi ngang ở vòng 6–8 (0,001578 → 0,001577). Mỗi vòng vẫn có host hai worker, count và `parameter_delta_l2 > 0`. Không lấy MSE test để chọn vòng; test của v2 đã được quan sát trước run nối tiếp, nên so sánh v2/v3 không còn là phép chọn mô hình với test hoàn toàn mù.

Run kiểm tra log trực tiếp ngày 16/10/2026 dùng model version `vitied-g05-ae64-live-20261016`, ba vòng trên đúng feature/split đã phát hành. Cả hai worker phát `worker_start`, 6 mốc batch mỗi vòng (batch đầu, mỗi 10 batch, batch cuối) và `worker_complete` qua stdout trong lúc huấn luyện. Mỗi vòng có 3.176/3.175 Post, 50 batch mỗi worker, `parameter_delta_l2 > 0`; validation MSE lần lượt 0,00188099, 0,00179881, 0,00172828; test MSE 0,00173437. [Console raw được ghi đồng thời](outputs/quality/g05_train_vitied-g05-ae64-live-20261016.console.txt) và [report phiên bản mới](outputs/quality/g05_train_vitied-g05-ae64-live-20261016.json). Đây là bằng chứng run mới; nó không thay đổi kết luận chất lượng trước đó rằng SVD có test MSE thấp hơn DL4J, và không làm G05 thành Topic classifier đã nghiệm thu.

## Đánh giá và quyết định sử dụng

| Biểu diễn 64 chiều | MSE test tái tạo | ARI cụm 8 nhóm qua hai seed, train | Silhouette validation |
|---|---:|---:|---:|
| SVD trên feature train-only | 0,001425 | 0,788 | 0,036 |
| DL4J v2 | 0,001734 | **0,967** | **0,053** |
| DL4J v3 | 0,001565 | 0,832 | 0,027 |

Nguồn phép đo: [đánh giá v2](outputs/quality/g05_evaluation_vitied-g05-ae64-v2.json) và [v3](outputs/quality/g05_evaluation_vitied-g05-ae64-v3.json). MSE của DL4J vẫn kém SVD. V2 là **ứng viên rà soát cụm** theo ARI/silhouette validation, không phải model Topic đã duyệt. Silhouette thấp ở cả ba cách biểu diễn; ARI chỉ đo độ lặp lại của thuật toán cụm, không chứng minh tên hoặc ranh giới chủ đề đúng. `creator_category` nguồn không được dùng làm nhãn test Topic.

[Tóm tắt 8 cụm v2](outputs/g05/cluster_summary_vitied-g05-ae64-v2.csv) và [80 Post mẫu](outputs/g05/cluster_review_vitied-g05-ae64-v2_mixed.csv) gồm 5 Post gần tâm + 5 Post chọn ngẫu nhiên mỗi cụm. [Phiếu rà soát](data/labels/vitied_g05_review_mixed.csv) có trường trống `fits_cluster`, `reviewed_topic_ids`, `reviewer`, `reviewed_at_utc`, `notes`; chưa có hàng nào được duyệt. Người rà soát nên đánh `yes/no/unclear`, đặt tên/ID Topic theo nội dung **từng Post**, cho phép nhiều Topic hoặc unknown. Không điền `creator_category` hoặc cluster ID làm nhãn mặc định. Khi có taxonomy và nhãn rà soát, mới đánh giá F1/coverage và cân nhắc classifier DL4J nhiều nhãn.

[Benchmark](outputs/quality/g05_benchmark.json) cùng 6.351 Post, ba vòng, batch 64, cache/container ấm: **một worker 53,4 giây**, **hai worker 73,5 giây**; validation MSE cuối là 0,001691 và 0,001728. Hai worker đã huấn luyện thật nhưng không tạo speedup trên bộ dữ liệu nhỏ, cùng host và một core/worker. Benchmark này chỉ đo một lần, chưa đủ báo variance/cold-start hoặc kết luận khả năng mở rộng.

## Lệnh chạy

## Release caption-only 16/10/2026

Do feature cũ gồm hashtag có thể làm nhiễu diễn giải topic, đã tạo feature/model mới **chỉ dùng caption** và giữ nguyên split author/caption leakage-safe. Mọi ID/split giữ nguyên so với `vitied-g05-hashidf512-v3`: train/validation/test 6.597/1.095/1.422; caption vector usable 5.947/1.023/1.284, tổng 8.254; 860 Post abstain (781 thiếu caption, 79 vector rỗng). IDF vẫn fit trên train-only.

Feature [manifest](outputs/quality/g05_features_vitied-g05-caption-idf512-v1.json) ghi `feature_mode=caption_only`. Autoencoder DL4J 512–128–64–128–512 chạy ba vòng trên hai JVM worker, 47 batch mỗi worker/vòng. Validation MSE giảm 0,002241 → 0,001661; test MSE 0,001655. Log thật từ hai worker: [console](outputs/quality/g05_train_vitied-g05-captiononly-ae64-live-20261016.console.txt); [train report](outputs/quality/g05_train_vitied-g05-captiononly-ae64-live-20261016.json).

So sánh cùng feature/split: SVD test MSE 0,001321 thấp hơn DL4J 0,001655; ARI giữa hai seed DL4J 0,982 so với SVD 0,753; silhouette validation 0,078 so với 0,060. Ranh giới cluster vẫn yếu; đây là khám phá cohort và review captions, chưa phải Topic classifier. [Đánh giá](outputs/quality/g05_evaluation_vitied-g05-captiononly-ae64-live-20261016.json), [dashboard artifact](outputs/quality/g05_model_dashboard_vitied-g05-captiononly-ae64-live-20261016.json).

```powershell
docker compose -f infra/compose.yaml exec -T spark-driver /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client jobs/spark/05_export_vitied_posts.py --silver /trend/vitied/silver/vitied-silver-v3/vitied-20261013-370234d0358b --output /workspace/outputs/g05/content/vitied-20261013-370234d0358b
python scripts/prepare_g05_features.py --content outputs/g05/content/vitied-20261013-370234d0358b --output outputs/g05/features/vitied-g05-hashidf512-v3
docker compose -f infra/compose.dl4j.yaml run --rm dl4j mvn -q package dependency:copy-dependencies
python scripts/train_g05_distributed.py --features outputs/g05/features/vitied-g05-hashidf512-v3 --output outputs/g05/models/vitied-g05-ae64-v2 --rounds 3 --batch-size 64
python scripts/evaluate_g05.py --features outputs/g05/features/vitied-g05-hashidf512-v3 --model outputs/g05/models/vitied-g05-ae64-v2 --content outputs/g05/content/vitied-20261013-370234d0358b
```

## Xem log train để demo

Các lần train thật ngày 14/10 đã lưu lịch sử từng vòng, host worker, số mẫu/batch, độ thay đổi tham số và validation MSE trong JSON. Raw stdout riêng của lần chạy cũ không được lưu thành `.log`; script dưới đây đọc manifest đó và in lại **lịch sử đã ghi nhận**, không giả lập hoặc train lại:

```powershell
python scripts/show_g05_training.py outputs/quality/g05_train_v2.json
python scripts/show_g05_training.py outputs/quality/g05_train.json
```

Trong lệnh train, mỗi JVM worker in tiến độ thật sau batch 1, mỗi 10 batch và batch cuối. Python đọc stdout/stderr đồng thời từ cả hai lệnh `docker compose exec`, in tức thời ra terminal với nhãn `[spark-worker-1]`/`[spark-worker-2]`, và lưu vào file console theo model version. Vì lệnh Java là `docker compose exec`, Docker Desktop tab Logs (log tiến trình chính của container) không nhận stdout của lệnh exec; terminal train và file console là nơi xem log này. JSON report vẫn lưu metric cuối kỳ, nhưng không dùng thay cho log tiến độ.

```powershell
docker compose -f infra/compose.yaml up -d spark-driver
docker compose -f infra/compose.dl4j.yaml run --rm dl4j mvn -q package dependency:copy-dependencies
python -u scripts/train_g05_distributed.py --features outputs/g05/features/vitied-g05-hashidf512-v3 --output outputs/g05/models/vitied-g05-ae64-v4 --rounds 3 --batch-size 64
```

Lệnh train trên tự hiện log trực tiếp ở cửa sổ PowerShell đang chạy. Nếu muốn xem file được ghi trực tiếp trong một cửa sổ khác, chạy:

```powershell
Get-Content outputs/quality/g05_train_vitied-g05-ae64-v4.console.txt -Wait
```

Lệnh đầu khởi động các dependency HDFS/Spark cần cho worker/driver; nó không xóa volume. Trainer tự lưu stdout/stderr thực của JVM tại `outputs/quality/g05_train_vitied-g05-ae64-v4.console.txt`; JSON report được tạo tại `outputs/quality/g05_train_vitied-g05-ae64-v4.json`, model ở `outputs/g05/models/vitied-g05-ae64-v4/`. Có thể chọn file khác bằng `--log-file <path>`; file phải chưa tồn tại để tránh ghi đè log cũ. Một run mới tiêu thụ CPU/RAM và tạo model version mới. Lệnh `show_g05_training.py` chỉ xem lại metric của run cũ, không train và không tái tạo stdout cũ.

Các release output không ghi đè; muốn chạy lại phải dùng version/path mới. Khi tiếp tục từ checkpoint, dùng `--initial-model <release>/best.zip`. Hai test G05 kiểm tra nhóm tác giả có caption trùng và Unicode; `py_compile` các script đạt. Neo4j vẫn giữ graph G04; chưa tạo `Topic` hay `HAS_TOPIC` từ cluster.
