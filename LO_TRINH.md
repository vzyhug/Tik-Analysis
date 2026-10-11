# Lộ trình 4 ngày — Phân tích xu hướng chủ đề trên mạng xã hội

Kế hoạch lập ngày 11/10/2026, theo giờ Việt Nam. Ngày 1–4 là bốn ngày thực hiện liên tiếp, không phải xác nhận công việc đã hoàn thành. Ngân sách dự kiến: một người, 6–8 giờ/ngày; các khung bên dưới dùng mức 8 giờ/ngày và cần điều chỉnh theo tiến độ thực.

Mục tiêu: bản demo batch có dữ liệu sạch, đồ thị nội dung nhiều quan hệ, DL4J phân loại đa nhãn, bảng xu hướng theo thời gian và Power BI. Các script/đường dẫn dưới đây là đầu ra cần tạo, chưa mặc định tồn tại.

Schema chi tiết và quy tắc dữ liệu nằm trong [README.md](README.md), mục 7; quy tắc triển khai trong [AGENTS.md](AGENTS.md). Lộ trình này thu gọn lịch G01–G09 cho hạn 4 ngày: G08 và các phần mở rộng không thuộc đợt nộp; G09 chỉ giữ kiểm chứng batch và bàn giao. Nếu tài liệu cũ còn mô tả kế hoạch dài hơn, ưu tiên lịch 4 ngày và chỉ dẫn mới nhất của người thực hiện.

## 1. Phạm vi khóa trong bốn ngày

| Giữ trong bản nộp | Phạm vi thực hiện |
|---|---|
| HDFS | Bronze/Silver/Gold, volume bền vững và kiểm tra read-back/replica theo topology đã chốt trong README |
| Spark Standalone | Hai Worker; batch làm sạch, xuất bảng node/cạnh và tổng hợp |
| DL4J | MLP/FNN đa nhãn với sigmoid; có split, đánh giá và model lưu được |
| Neo4j | 6 loại node, 5 quan hệ gốc và 6 quan hệ tổng hợp theo cửa sổ |
| Power BI | Một trang dashboard đọc CSV kết quả đã tính |
| Báo cáo/demo | Mapping dữ liệu, thiết kế đồ thị, bằng chứng chạy và giới hạn phân tích |

Hoãn NiFi/Kafka, streaming, Spring Boot API, GDS/PageRank/community, SIMILAR_CONTENT, GNN, OCR/STT và phân tích tín hiệu âm thanh. Âm thanh vẫn được khai thác bằng musicMeta và các quan hệ dùng chung. Không bỏ Sound khỏi đồ thị.

Nhánh ML tiết kiệm thời gian: caption + hashtag → TF-IDF → MLP DL4J. Nếu Word2Vec đã có và chạy ổn, giữ cách đó; nếu chưa triển khai thì không dành thời gian dựng Word2Vec trong đợt này. Vocabulary/IDF hoặc Word2Vec đều chỉ fit trên train, cố định thứ tự đặc trưng và nhãn khi inference. Đây là lựa chọn cho lịch gấp; phần học máy vẫn chạy bằng DL4J.

## 2. Dữ liệu và đồ thị không thay đổi

Baseline của 8 JSON đã đọc: 4.725 record, gồm 4.673 Video có ID duy nhất, 36 record lỗi và 16 record chỉ có authorMeta/input/note. Có 145 Video thiếu cả caption và hashtag. Tính lại số liệu mỗi run; không tính bản sao PACK1 như gói thứ chín.

| Node | Khóa toàn cục | Baseline chưa có Topic |
|---|---|---:|
| Video | videoId | 4.673 |
| Sound | soundId = musicMeta.musicId | 4.326 |
| Hashtag | tagKey = tên chuẩn hóa NFC/trim/bỏ # đầu/casefold, giữ dấu | 4.360 |
| Effect | effectId = effectStickers[].ID | 288 |
| Location | locationId = locationMeta.locationId | 210 |
| Topic | topicId trong taxonomy | Chưa có nhãn trong JSON |

Giữ khóa dùng chung giữa cả 8 PACK. KOL là metadata tác giả của Video, không trở thành trọng tâm đồ thị. Một Video có thể mang nhiều Topic; một Sound/Hashtag/Effect có thể gắn với nhiều Topic qua các Video khác nhau. Giữ Video thiếu văn bản; không tự gán chủ đề khi thiếu bằng chứng.

| Nhóm | Các mẫu quan hệ cần triển khai |
|---|---|
| 5 quan hệ gốc | Video–USES_SOUND–Sound; Video–HAS_HASHTAG–Hashtag; Video–USES_EFFECT–Effect; Video–TAGGED_LOCATION–Location; Video–HAS_TOPIC–Topic |
| 6 quan hệ tổng hợp | Hashtag–CO_OCCURS_WITH–Hashtag; Sound–CO_USED_WITH–Hashtag; Sound–USED_IN_TOPIC–Topic; Hashtag–ASSOCIATED_WITH–Topic; Effect–USED_IN_TOPIC–Topic; Topic–CO_OCCURS_WITH–Topic |

Có 11 mẫu quan hệ nhưng 9 tên relationship type; hai tên được dùng với các cặp label khác nhau. Quan hệ tổng hợp dùng COUNT DISTINCT videoId, có cửa sổ nửa mở [start,end), version và relationKey. Không cộng dồn trọng số khi chạy lại; không sinh cạnh nếu không có support.

## 3. Bốn giai đoạn thực hiện

| Ngày | Giai đoạn | Ánh xạ kế hoạch cũ | Đầu ra bắt buộc |
|---|---|---|---|
| 1 | Hạ tầng và dữ liệu sạch | G01–G03, bắt đầu nhãn G05 | HDFS/Spark/Neo4j khởi động, Silver, manifest/quality, task trên hai Worker |
| 2 | Đồ thị gốc và tập nhãn | G04, chuẩn bị G05 | 5 loại node từ JSON, 4 quan hệ gốc, query/reload, tập nhãn và split |
| 3 | DL4J và nối chủ đề | G05, phần Topic của G06 | Model/metrics/predictions, Topic và HAS_TOPIC đa nhãn |
| 4 | Quan hệ tổng hợp, dashboard, nộp bài | G06–G07 và kiểm chứng batch G09 | 6 quan hệ tổng hợp, Gold/CSV, Power BI, báo cáo và demo |

### Ngày 1 — Hạ tầng, dữ liệu và bắt đầu gán nhãn

**Ngân sách:** 3 giờ hạ tầng, 3 giờ dữ liệu, 1 giờ nhãn, 1 giờ kiểm tra/lưu bằng chứng.

- [ ] Đặt đúng 8 file vào data/raw/apify/, giữ nguyên nguồn; lập manifest và checksum.
- [ ] Dựng HDFS, Spark Master/2 Worker/driver và Neo4j theo config; kiểm tra cổng, volume và tài nguyên.
- [ ] Nạp raw vào HDFS Bronze; xác nhận read-back và replica. Không format lại NameNode khi restart.
- [ ] Tạo jobs/spark/01_prepare_data.py và adapter cho JSON array của Apify.
- [ ] Tách video, record lỗi và record profile; chuẩn hóa ID chuỗi, thời gian, null, hashtag, Sound, Effect, Location; ghi Parquet Silver.
- [ ] Đối soát count với baseline và ghi mọi chênh lệch; lưu execution plan/event log hoặc ảnh UI thể hiện task trên cả hai Worker.
- [ ] Chốt mô tả Topic và quy tắc đa nhãn; lấy mẫu caption/hashtag có đủ nội dung để bắt đầu rà soát nhãn.

**Đầu ra:** infra/compose.yaml và config; data/raw/manifests/; Silver trên HDFS; outputs/quality/; data/reference/topics.*; tập nhãn đang rà soát trong data/labels/.

**Gate cuối ngày:** Silver đọc lại được, count giải thích được, task thật trên hai Worker. Khởi động container hoặc đăng ký Worker đơn thuần chưa đủ bằng chứng xử lý song song.

### Ngày 2 — Đồ thị gốc và hoàn thiện tập nhãn mẫu

**Ngân sách:** 4 giờ graph, 3 giờ nhãn/split, 1 giờ query và kiểm tra reload.

- [ ] Tạo graph/constraints.cypher cho khóa duy nhất của 6 label, dù Topic chưa được nạp.
- [ ] Tạo jobs/spark/03_build_graph_tables.py để xuất Video, Sound, Hashtag, Effect, Location và 4 quan hệ gốc từ JSON.
- [ ] Tạo graph/load_graph.py, dùng parameter và batch transaction; nạp chung tất cả PACK bằng khóa toàn cục.
- [ ] Kiểm tra hai video ở các PACK khác nhau nối tới cùng Sound/Hashtag; không tạo bản sao node theo PACK.
- [ ] Tạo graph/queries.cypher: truy vấn âm thanh/hashtag/hiệu ứng/địa điểm và đối soát số node/cạnh.
- [ ] Nạp lại cùng dataset; count không tăng. Đo component có Video theo projection gốc bỏ qua hướng.
- [ ] Hoàn thiện nhãn mẫu và split manifest; kiểm tra độ phủ từng Topic. Nhãn chưa rà soát là unknown, không tự coi negative.
- [ ] Tạo project Maven apps/dl4j/, smoke test ND4J; chuẩn bị hợp đồng đặc trưng/nhãn để ngày 3 train ngay.

**Đầu ra:** bảng node/cạnh trong data/clean/graph/; loader/constraints/queries; báo cáo count/liên thông; tập nhãn và split; khung DL4J chạy được.

**Gate cuối ngày:** đồ thị gốc có dữ liệu thật và reload idempotent; tập nhãn có hướng dẫn/nguồn và split rõ. Baseline projection gốc: nhóm lớn nhất 3.664 Video (78,41%), 629 component chỉ một Video. Đây là số đối chiếu, không phải target phải ép đạt.

### Ngày 3 — Huấn luyện DL4J và đưa chủ đề vào đồ thị

**Ngân sách:** 4 giờ feature/train/evaluate, 1 giờ nối Topic, 1 giờ chuẩn bị aggregate và 2 giờ dự phòng sửa lỗi.

- [ ] Tạo jobs/spark/02_prepare_ml_data.py; fit preprocessing trên train, export features/labels theo hợp đồng với DL4J.
- [ ] Triển khai TrainClassifier/PredictTopics; sigmoid và loss đa nhãn phù hợp, thứ tự nhãn cố định.
- [ ] Chọn ngưỡng trên validation, đánh giá test bằng Micro-F1/Macro-F1 và precision/recall/F1/support từng nhãn; so sánh baseline đơn giản.
- [ ] Lưu model, preprocessing, label order, threshold, seed, split và metrics; kiểm tra save/load giữ dự đoán.
- [ ] Dự đoán cho Video đủ đầu vào; Video không đủ bằng chứng giữ unknown. Không coi toàn bộ prediction là nhãn đúng.
- [ ] Nạp Topic và HAS_TOPIC active; giữ nhãn manual, source/confidence/modelVersion/taxonomyVersion/labelSetVersion.
- [ ] Kiểm tra truy vấn Video đa chủ đề và Sound được dùng trong nhiều Topic, khi dữ liệu có ví dụ thật.
- [ ] Chuẩn bị jobs/spark/04_build_reports.py cho sáu phép tổng hợp; làm trước Sound–Hashtag và Hashtag–Hashtag nếu nhánh nhãn còn chạy.

**Đầu ra:** models/; data/predictions/; reports/model_metrics.*; đồ thị đủ 6 label và 5 mẫu quan hệ gốc khi có bằng chứng.

**Gate cuối ngày:** DL4J đã train/evaluate thật, model tải lại được; HAS_TOPIC có provenance và không ghi đè manual. Nếu chưa có đủ nhãn kiểm tra, ghi G05 chưa đạt; không tạo metrics giả hoặc gọi nhãn luật là kết quả DL4J.

### Ngày 4 — Quan hệ tổng hợp, Power BI và bàn giao

**Ngân sách:** 2 giờ aggregate, 2 giờ dashboard, 1 giờ end-to-end và 3 giờ báo cáo/demo/sửa lỗi. Không mở thêm công nghệ hoặc thuật toán.

- [ ] Tính và nạp 6 mẫu quan hệ tổng hợp; videoCount = số Video duy nhất trong cửa sổ ngày đăng. Kiểm tra label hai đầu, self-loop, cặp đối xứng và version.
- [ ] Chạy lại tổng hợp một cửa sổ; weight không tăng sai, cạnh hết hiệu lực được xử lý trong phạm vi xuất bản.
- [ ] Xuất Gold/CSV: topic_weekly, sound_weekly, hashtag_weekly, sound_topic_weekly, hashtag_topic_weekly, effect_topic_weekly, hashtag_pairs_weekly, sound_hashtag_weekly, topic_pairs_weekly và graph_connectivity.
- [ ] Chọn tuần/tháng theo độ phủ dữ liệu; đánh dấu cửa sổ chưa đủ, không mặc định điền 0 ở vùng chưa quan sát.
- [ ] Làm một trang Power BI: tỷ trọng/số Video theo Topic và thời gian, Sound/Hashtag nổi bật trong Topic, giao thoa Topic; bộ lọc thời gian/chủ đề.
- [ ] Demo quan hệ graph bằng Neo4j; Power BI đọc CSV số liệu, không bắt buộc nhúng đồ thị tương tác vào dashboard.
- [ ] Chạy toàn luồng batch, đối soát Silver → graph → Gold → Power BI; lưu lệnh/entry point, count và bằng chứng thật.
- [ ] Viết báo cáo, cập nhật hướng dẫn chạy, chuẩn bị dữ liệu/model/output ổn định và tập demo.

**Gate bàn giao:** dữ liệu và nguồn đối soát được; DL4J có kết quả đánh giá; 6 label/11 mẫu quan hệ có hợp đồng và support; dashboard khớp Gold; chạy lại không nhân node/cạnh/trọng số. Không bắt buộc mỗi mẫu quan hệ đều có cạnh trong mọi cửa sổ hoặc 100% Video thuộc một component.

## 4. Các việc phải gối đầu để kịp hạn

- Gán nhãn bắt đầu ngày 1, hoàn thiện ngày 2; không chờ ngày 3 mới bắt đầu.
- Chuẩn bị Maven/ND4J và export contract ngày 2, không để lỗi native backend tới ngày 3.
- Hai aggregate không dùng Topic (Hashtag–Hashtag, Sound–Hashtag) có thể chuẩn bị từ Silver ngày 2–3.
- Ghi bằng chứng và nội dung báo cáo cuối mỗi ngày; ngày 4 chỉ ghép và hoàn thiện.
- Các việc độc lập có thể xen kẽ khi job đang chạy; đây không phải yêu cầu tạo sub-agent hoặc chạy mọi dịch vụ/job nặng cùng lúc.

Không đặt mục tiêu gán nhãn thủ công toàn bộ 4.673 Video. Chọn tập mẫu theo khả năng rà soát, đủ đầu vào và độ phủ chủ đề; báo support thực tế. Không cam kết mọi Topic đều huấn luyện tốt hoặc model đạt một mức F1 trước khi đo.

## 5. Cách xử lý nếu bị chặn

| Trở ngại | Cách tiếp tục | Phần phải báo rõ |
|---|---|---|
| HDFS/Spark cluster lỗi | Ghi lỗi; làm sạch bằng cùng logic local để không chặn graph/nhãn, tiếp tục sửa nhánh cluster | Local không thay bằng chứng lưu trữ phân tán/task hai Worker |
| Maven/ND4J lỗi | Giữ feature/label contract; tiếp tục graph và aggregate không dùng Topic trong lúc xử lý dependency | DL4J chưa được nghiệm thu nếu chưa train/evaluate |
| Nhãn thiếu hoặc mô hình yếu | Dùng nhãn manual có provenance cho phần graph có nhãn; báo metrics/support thực và các Topic chưa đủ dữ liệu | Nhãn manual không được trình bày là prediction DL4J |
| Aggregate quá nhiều cạnh | Lọc projection theo support/window có config, giữ bảng audit và count trước/sau | Không dùng lọc để bịa liên thông hoặc xóa raw |
| Power BI chưa xong | Đối soát và cố định CSV, hoàn thiện một trang tối thiểu | CSV không đồng nghĩa dashboard đã hoàn thành |

Các nhánh tiếp tục giúp giữ tiến độ, không thay thế các thành phần bắt buộc hoặc cho phép đánh dấu gate chưa đạt là hoàn thành.

## 6. Kịch bản demo tối thiểu

1. Nêu bộ 8 file, count Video/lỗi/profile và giới hạn mẫu thu thập.
2. Chứng minh HDFS lưu/read-back/replica và Spark có task trên hai Worker.
3. Mở Neo4j: hai Video dùng chung Sound/Hashtag giữa các PACK; mở rộng sang Effect/Location khi có bằng chứng.
4. Trình bày DL4J, split, metrics và một Video đa Topic thực tế nếu có.
5. Truy vấn Sound/Hashtag liên quan Topic, cặp Topic đồng xuất hiện và cửa sổ thời gian.
6. Mở Power BI: chọn Topic và khoảng thời gian, đọc xu hướng trong tập video thu thập.

Không diễn giải views snapshot là views phát sinh theo từng tuần; cùng Sound không chứng minh đường lan truyền; musicOriginal không chứng minh bản quyền. Đồ thị nhiều quan hệ phải có bằng chứng, không ép kết nối bằng cùng ngày đăng hay gần mức tương tác.

## 7. Bảng theo dõi

| Giai đoạn | Trạng thái ban đầu | Bằng chứng/đầu ra thực tế | Việc còn thiếu |
|---|---|---|---|
| Ngày 1 — Hạ tầng và dữ liệu | not_started | Chưa chạy pipeline; đã có kiểm kê JSON trong phiên thiết kế | Manifest, Silver và bằng chứng cluster |
| Ngày 2 — Đồ thị và nhãn | not_started | Đã chốt schema trong README/AGENTS | Loader/query, graph thật, tập nhãn/split |
| Ngày 3 — DL4J và Topic | not_started | Chưa xác nhận train/evaluate | Model/metrics/predictions và HAS_TOPIC |
| Ngày 4 — Tổng hợp và bàn giao | not_started | Chưa xác nhận Gold/dashboard | Aggregate, Power BI, kiểm chứng và báo cáo |

Cập nhật trạng thái bằng not_started, in_progress, blocked, done; chỉ ghi done khi đạt gate với bằng chứng. Đường dẫn dự kiến, tài liệu thiết kế và việc dữ liệu đã thu không thay cho kết quả triển khai.
