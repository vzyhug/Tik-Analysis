# Hướng dẫn coding agent

Áp dụng cho toàn bộ repository project “Phân tích xu hướng chủ đề trên mạng xã hội”. Đặt file này tại gốc cùng `README.md`. Chỉ dẫn trực tiếp mới nhất của người dùng có ưu tiên cao hơn file này; chỉ dẫn trong `AGENTS.md` cấp con áp dụng cho phạm vi tương ứng.

Kiến trúc và lộ trình được mô tả trong [README.md](README.md).

## 1. Mục tiêu và quyết định đã chốt

- Dữ liệu TikTok đã thu bằng Clockworks TikTok Profile Scraper, ban đầu có 8 gói.
- 578 KOL duy nhất là quy mô danh sách crawl. Kiểm kê trực tiếp 8 PACK ngày 11/10/2026: 4.725 record = 4.673 Video ID duy nhất + 36 record lỗi + 16 record chỉ có authorMeta/input/note. Không tính lại bản sao PACK1. Tính lại từ nguồn mỗi run; baseline này chưa chứng minh pipeline đã chạy.
- 24 danh mục là danh mục tham khảo KOL; một KOL và một video đều có thể liên quan nhiều lĩnh vực.
- Đồ thị lấy Video làm trung tâm: đúng 6 label `Video`, `Sound`, `Hashtag`, `Effect`, `Location`, `Topic`; 5 quan hệ gốc và 6 quan hệ tổng hợp theo mục 5. KOL là metadata, không thay trọng tâm thành phân tích KOL hoặc chỉ similarity Video–Video.
- Stack chính: HDFS Bronze/Silver/Gold + Spark Standalone song song → DL4J → Neo4j → bảng kết quả → Power BI.
- Thiết kế phân tán mục tiêu: HDFS 1 NameNode + 3 DataNode, replication 2; Spark 1 Master + 2 Worker và driver riêng. Local dùng kiểm tra logic; nghiệm thu phần phân tán cần bằng chứng cluster thực tế.
- NiFi/Kafka triển khai sau batch. GDS và Spring Boot API bổ sung khi mốc tương ứng cần.
- Không dùng GNN trong thiết kế hiện tại. Không thay phần học máy DL4J bằng một framework khác mà không có chỉ dẫn thay đổi.
- Phương án ML đầu tiên: Word2Vec và FNN/MLP phân loại nhiều nhãn. Không tự nâng thành LSTM/diffusion/LLM hoặc huấn luyện phân tán khi chưa có nhu cầu và bằng chứng dữ liệu.

## 2. Khi bắt đầu một task

1. Đọc `README.md`, file này và các hướng dẫn cấp con liên quan.
2. Kiểm tra file, dependency, entry point và trạng thái Git thực tế; tìm bằng `rg`/`rg --files` khi có.
3. Phân biệt thiết kế dự kiến với phần đã triển khai. Không giả định các script trong README tồn tại.
4. Nếu có `rule.docx` hoặc tài liệu yêu cầu, đọc nội dung hiện tại và đối chiếu với yêu cầu người dùng. Ghi rõ trường chưa thu được hoặc định nghĩa còn mơ hồ.
5. Khi xử lý dữ liệu, kiểm tra schema của từng nguồn và số record. Nếu chưa có dữ liệu thật, dùng fixture có nhãn synthetic và báo giới hạn kiểm tra.
6. Giữ thay đổi người dùng đã làm, không reset/clean checkout hoặc sửa phần ngoài phạm vi task.

Ưu tiên làm xong một mốc nhỏ có đầu ra kiểm tra được. Tiếp tục các bước đã được người dùng giao; không hỏi lại cho các lựa chọn kỹ thuật thông thường, có thể đảo ngược và nằm trong phạm vi task.

### 2.1. Kế hoạch thống nhất G01–G09

Các giai đoạn sau là hợp đồng triển khai chung với README. Dùng các mã này trong task, log tiến độ và bàn giao; không tiếp tục dùng bộ mã mốc cũ.

| Giai đoạn | Phạm vi công việc | Bằng chứng để đánh dấu hoàn thành |
|---|---|---|
| G01 | Project, intake/schema, manifest, kiểm kê raw | Count thực, missing/duplicate/conflict, checksum và mapping |
| G02 | HDFS + Spark Standalone, Compose/config/network/volume | Read/write/replica, worker registration, restart giữ dữ liệu |
| G03 | Upload Bronze, Spark normalize và Parquet Silver | Đối soát và task thực trên cả hai worker; output tương đương local |
| G04 | Video/Sound/Hashtag/Effect/Location, constraints, loader/query | Khóa dùng chung 8 PACK, 4 quan hệ từ JSON, count/liên thông và reload idempotent |
| G05 | Taxonomy/nhãn, split, DL4J/baseline/evaluate/predict | Nhãn kiểm tra, không leakage, metrics, model save/load và prediction |
| G06 | HAS_TOPIC đa nhãn, 6 quan hệ tổng hợp, Gold và liên thông | 6 label/11 mẫu quan hệ có bằng chứng, active label/window/version/distinct count được kiểm tra |
| G07 | Export, Power BI, API tùy chọn | Dashboard/refresh/filter khớp Gold, không train trong request |
| G08 | NiFi/Kafka replay, streaming/checkpoint/sink | Restart/replay idempotent, timestamp nguồn, offset/lag và nhánh lỗi |
| G09 | End-to-end, failure demo thử riêng, benchmark, tài liệu/demo | Evidence từng tầng, giới hạn topology và hướng dẫn tái lập |

- M1 = G01–G03; M2 = G04–G07 dựa trên M1; M3 = G08–G09 dựa trên M2.
- Ưu tiên G01 → G02 → G03. Kiểm tra logic Spark local là bước hỗ trợ G03, không thay nghiệm thu task trên cụm.
- Sau G03, G04 và G05 độc lập theo hợp đồng dữ liệu; taxonomy/gán nhãn trong G05 có thể bắt đầu sau intake G01. Không sửa raw hoặc schema chung theo hai cách không tương thích.
- G06 đầy đủ cần G04/G05; thống kê hashtag có thể làm sớm. G07 có thể dựng khung trước bằng dữ liệu chuẩn và phải nối/đối soát kết quả cuối.
- G08 là phần mở rộng sau batch; GDS trong G06 và Spring Boot API trong G07 chỉ làm khi cần/chọn. Không để các phần tùy chọn chặn đầu ra chính của M1/M2.
- Chạy kiểm tra trong từng giai đoạn; G09 không phải lý do hoãn kiểm tra dữ liệu/đồ thị/ML đến cuối.
- Phụ thuộc task không phải chỉ dẫn tự tạo sub-agent hoặc bật nhiều job nặng đồng thời. Điều phối theo tài nguyên và chỉ dẫn người dùng.

### 2.2. Theo dõi tiến độ và xử lý trở ngại

- Khi bắt đầu triển khai pipeline, tạo/cập nhật `reports/progress.md` theo G01–G09 với trạng thái `not_started`, `in_progress`, `blocked`, `done`.
- Ghi đầu ra, lệnh/kiểm tra đã chạy, bằng chứng, phần chưa đạt và task tiếp theo. Đường dẫn dự kiến không phải bằng chứng file đã tồn tại.
- Chỉ đánh dấu `done` sau điều kiện nghiệm thu. Dữ liệu đã thu không đồng nghĩa G01 đã kiểm kê; file Java có sẵn không đồng nghĩa G05 đã huấn luyện/đánh giá.
- Nếu thiếu nhãn, Docker, dữ liệu hoặc tài nguyên, ghi task bị chặn và giới hạn; tiếp tục phần độc lập đã được giao. Không tự tạo số liệu thật/metrics giả để vượt gate.
- Khi chỉ được giao một giai đoạn, hoàn thành phạm vi đó cùng kiểm tra và tài liệu liên quan; không tự mở rộng sang toàn pipeline hoặc dịch vụ có phí.
- Khi người dùng thay đổi thứ tự/phạm vi, cập nhật README và progress để phản ánh quyết định mới, giữ các ràng buộc dữ liệu còn áp dụng.

## 3. Phân chia mã nguồn

| Thư mục | Trách nhiệm |
|---|---|
| `jobs/spark/` | Đọc/chuẩn hóa, join nhãn, xuất đặc trưng, thống kê và bảng đồ thị |
| `apps/dl4j/` | Java Maven: biểu diễn, huấn luyện, đánh giá và inference |
| `graph/` | Constraints, Neo4j loader, queries và graph analytics |
| `infra/` | Docker Compose, cấu hình container và NiFi flow |
| `config/` | Mapping schema, taxonomy, timezone, tham số pipeline |
| `scripts/` | Wrapper chạy từ Windows, có kiểm tra lỗi |
| `tests/` | Fixture và kiểm tra hành vi quan trọng |
| `outputs/` | Bảng và báo cáo sinh ra; không chứa logic nghiệp vụ |

Entry point ngắn, logic dùng lại đặt trong module; batch và streaming gọi chung hàm chuẩn hóa. DL4J và Spark trao đổi bằng hợp đồng dữ liệu có phiên bản, không nhét tất cả vào cùng một JVM chỉ để tích hợp.

## 4. Quy tắc dữ liệu bắt buộc

- `data/raw/` bất biến đối với các job. Không chỉnh sửa/ghi đè export nguồn.
- Đọc cả 8 gói thành một dataset logic, nhưng giữ `source_file`, `collection_id`, schema version và checksum nguồn.
- Không đoán tên cột Apify. Kiểm tra JSON/JSONL/CSV thực tế và quản lý adapter/mapping bằng config.
- ID video, author, music và ID nguồn duet/stitch là chuỗi. Không chuyển qua floating point.
- Timestamp chuẩn lưu UTC; lịch ngày/tuần dùng timezone cấu hình, mặc định `Asia/Ho_Chi_Minh`. Week key phải có năm và ngày bắt đầu tuần.
- `create_time` là thời gian đăng, `observed_at` là thời gian quan sát. `ingested_at` và `replayed_at` là trường khác.
- Thiếu trường giữ `null`. Cờ boolean không có bằng chứng không được gán `false`; số tương tác thiếu không được gán 0.
- Phân biệt mảng rỗng đã xác nhận với mảng không lấy được. Không bỏ record chỉ vì thiếu caption, hashtag hoặc nhạc; đánh dấu giới hạn sử dụng tương ứng.
- Cờ `is_original_sound` mô tả metadata nguồn, không chứng minh bản quyền.
- Nếu chỉ có username, dùng fallback có namespace và trạng thái phân giải; không trộn với author ID. Username mention không chứng minh tài khoản đã được thu hoặc tồn tại.
- Hash/manifest phải ổn định khi chạy lại. Không dùng tên file làm lần thu mới nếu thực chất chỉ là bản sao một export.
- Với cùng `video_id + collection_id`, loại bản sao chính xác; xung đột khác nội dung phải có báo cáo/quy tắc lựa chọn, không âm thầm dùng bản cuối.
- Giữ nhiều bản ghi quan sát trong Silver/source khi có các lần thu độc lập; không thêm node Observation vào schema chính. Nếu không biết `observed_at`, giữ trạng thái chưa biết; không dùng timestamp import như lịch sử nguồn.
- Record lỗi đi quarantine kèm source pointer và lý do. Đối soát accepted, duplicate, conflict và rejected theo quy tắc đếm đã công bố.

## 5. Schema và đồ thị

### 5.1. Schema bắt buộc và mapping

| Label | Khóa duy nhất | Nguồn |
|---|---|---|
| Video | videoId | id |
| Sound | soundId | musicMeta.musicId |
| Hashtag | tagKey | hashtags[].name chuẩn hóa |
| Effect | effectId | effectStickers[].ID |
| Location | locationId | locationMeta.locationId |
| Topic | topicId | Taxonomy; nhãn kiểm tra hoặc kết quả DL4J |

- Không tự thêm Author, Category, Music, Observation, Week hoặc node trung gian để ép đồ thị liên thông. Label âm thanh là Sound. Tác giả/mentions giữ metadata/Silver; lịch sử snapshot giữ ngoài schema graph chính. Mở rộng schema chỉ khi có chỉ dẫn mới.
- Khóa toàn cục giữa cả 8 PACK, không có namespace PACK trong khóa thực thể. Không tạo Sound/Hashtag riêng theo Video. Không gộp âm thanh theo tên/tác giả nhạc.
- ID là chuỗi; Hashtag chuẩn hóa NFC → trim → bỏ # đầu → casefold, giữ dấu tiếng Việt. Không dùng hashtags[].id vì có thể null; bỏ trùng tag/effect trong từng Video.
- Tạo uniqueness constraints trước load; MERGE theo khóa và cập nhật thuộc tính riêng. Không dùng caption/nickname làm ID.
- Mapping thuộc tính Video, Sound, Effect, Location theo README mục 7. Flatten dữ liệu lồng nhau trước load; không lưu map/list-of-map như property Neo4j. Metadata thực thể dùng chung xung đột phải có quy tắc lựa chọn và báo cáo.
- publishedAt là thời gian đăng; tương tác là snapshot. musicOriginal không chứng minh bản quyền; TAGGED_LOCATION không chứng minh nơi quay. URL âm thanh/phụ đề không đồng nghĩa đã tải hoặc có transcript.
- Không bỏ Video thiếu caption/hashtag/nhạc. Thiếu nội dung thì Topic unknown; không gán nhãn giả để nối component.

### 5.2. Năm quan hệ gốc

| Từ | Quan hệ | Đến |
|---|---|---|
| Video | USES_SOUND | Sound |
| Video | HAS_HASHTAG | Hashtag |
| Video | USES_EFFECT | Effect |
| Video | TAGGED_LOCATION | Location |
| Video | HAS_TOPIC | Topic |

- Cạnh gốc duy nhất theo loại quan hệ và hai khóa node. Video có 0..n Topic/Hashtag/Effect, 0..1 Sound/Location theo schema nguồn hiện tại.
- Topic không sao chép máy móc danh mục KOL. Một Sound/Hashtag/Effect có thể liên quan nhiều Topic qua các Video khác nhau.
- Một Video–Topic có một cạnh active: source (manual/dl4j), confidence nếu có, modelVersion, taxonomyVersion, labelSetVersion. Score không mặc định là xác suất hiệu chuẩn.
- Lịch sử prediction/nhãn lưu ngoài cạnh active. Inference không xóa nhãn manual. Khi đổi model/threshold, đồng bộ cạnh dự đoán active gồm bỏ cạnh dự đoán hết hiệu lực đúng phạm vi; không xóa toàn graph.
- Không suy diễn follow/share/duet/stitch/lan truyền từ tổng tương tác, mentions, cùng hashtag hoặc cùng âm thanh. Không tự tạo cạnh không thuộc schema đã chốt.

### 5.3. Sáu quan hệ tổng hợp

Schema có 11 mẫu quan hệ nhưng 9 tên relationship type. CO_OCCURS_WITH và USED_IN_TOPIC dùng với hai cặp label khác nhau; kiểm tra/query phải xét label hai đầu.

| Hai đầu | Quan hệ | Điều kiện |
|---|---|---|
| Hashtag–Hashtag | CO_OCCURS_WITH | Video có cả hai tag |
| Sound–Hashtag | CO_USED_WITH | Video có sound và tag |
| Sound–Topic | USED_IN_TOPIC | Video có sound và Topic active |
| Hashtag–Topic | ASSOCIATED_WITH | Video có tag và Topic active |
| Effect–Topic | USED_IN_TOPIC | Video có effect và Topic active |
| Topic–Topic | CO_OCCURS_WITH | Video có cả hai Topic active |

- videoCount/weight = số Video duy nhất làm bằng chứng trong cửa sổ ngày đăng, không phải số đường đi, số snapshot hoặc tổng lượt xem. Bỏ trùng các cặp trong từng Video.
- Lưu videoCount, windowStart/windowEnd, datasetVersion, labelSetVersion nếu dùng nhãn, aggregationVersion và relationKey. Cửa sổ nửa mở [start,end), xác định theo Asia/Ho_Chi_Minh rồi lưu UTC.
- relationKey ổn định gồm loại quan hệ, label/khóa hai đầu, cửa sổ và các version liên quan. Cặp không hướng lưu một cạnh theo thứ tự canonical; không self-loop, không tạo hai cạnh đối xứng. Phân biệt CO_OCCURS_WITH Hashtag–Hashtag và Topic–Topic bằng label.
- Support threshold cấu hình riêng từng loại. Mức >=3 Video là gợi ý projection hashtag, không hardcode cho mọi loại. Không buộc sinh cạnh nếu thiếu support. Giữ bảng đầy đủ để audit.
- Chạy lại dùng upsert/replace phạm vi, không cộng dồn weight. Khi bằng chứng/nhãn/cửa sổ đổi, bỏ cạnh tổng hợp hết hiệu lực trong phạm vi xuất bản; giữ version lịch sử có chỉ định, không trộn vào projection active.
- Giữ tập bằng chứng/mapping trong bảng nguồn để truy vết; không nhét danh sách Video lớn vào một property của cạnh.

### 5.4. Liên thông, projection và phạm vi bổ sung

- Đo liên thông trên projection gốc bỏ qua hướng, projection thêm Topic, projection aggregate hoặc similarity riêng; ghi label/type/filter/window/version. Không trộn mọi cạnh vào một phép PageRank/cộng đồng mà không giải thích.
- Baseline 8 file: 4.673 Video, 4.326 Sound, 4.360 Hashtag, 288 Effect, 210 Location; cạnh gốc JSON tương ứng 4.673/14.517/3.246/392. Projection gốc có 693 component có Video, nhóm lớn nhất 3.664 Video (78,41%), 629 component chỉ một Video. Tính lại từ file và báo chênh lệch; đây không phải node không có cạnh hoặc target phải ép đạt.
- Nối Topic từ nội dung là cầu nối có thể thêm giữa nhóm Video. Cạnh tổng hợp từ các đường đi cũ không tự nối component mới. Không tuyên bố 100% liên thông trước đo.
- Hashtag chung như fyp/xuhuong giữ trong dữ liệu; lọc/giảm ảnh hưởng trong projection ngữ nghĩa theo config, kèm báo cáo. Không nối vì cùng tuần, tương tác gần nhau hoặc chỉ để làm hình đẹp.
- SIMILAR_CONTENT là tùy chọn ngoài 11 mẫu quan hệ bắt buộc. Nếu được triển khai, dùng đặc trưng caption/hashtag có bằng chứng, ngưỡng kiểm tra và top-k; lưu featureVersion/score/threshold/lý do. Không ép mỗi Video có hàng xóm; không lấy similarity làm trọng tâm thay xu hướng chủ đề.
- GDS/APOC không mặc định có sẵn. Không tự cài/mở rộng để chặn bản batch. Node Similarity theo hàng xóm và tương đồng văn bản là hai phép khác nhau; ghi rõ phương pháp.

## 6. Học máy và chống rò rỉ dữ liệu

- Huấn luyện có giám sát yêu cầu nhãn Video kiểm tra được, taxonomy và hướng dẫn gán nhãn. Nhãn không được rà soát là unknown, không mặc định negative.
- Pseudo-label/rule label có provenance; không dùng làm ground truth của test mà không công bố.
- Chọn split trước fit preprocessing. Không chia theo 8 gói Apify. Lưu split manifest theo `video_id` và lý do chọn phương pháp.
- Khi kiểm tra generalization sang KOL mới, giữ author độc lập giữa split. Khi đánh giá tương lai, chia thời gian và giữ thứ tự phù hợp.
- Snapshot và bản sao cùng Video luôn ở cùng split; xử lý near-duplicate nếu có dấu hiệu ảnh hưởng đánh giá.
- Vocabulary, Word2Vec, scaler và các bước fit khác chỉ học trên train. Vector tiền huấn luyện bên ngoài phải có nguồn và license; không mô tả nó là mô hình được học từ dataset này.
- Sigmoid theo nhãn với loss nhiều nhãn phù hợp; cố định thứ tự nhãn giữa training và inference. Nếu nhãn chỉ được rà soát một phần, xử lý mask hoặc chọn subset đủ nhãn.
- Threshold chọn trên validation; không tối ưu trên test. Score chưa mặc định được hiệu chuẩn thành xác suất.
- Không đưa số liệu tương lai vào feature. Nếu mở rộng dự báo tăng tương tác, target lấy từ bản ghi quan sát nguồn sau trong Silver; feature chỉ dùng thông tin có tại thời điểm dự đoán.
- Báo cáo baseline, Micro-F1, Macro-F1, precision/recall/F1 từng nhãn và support. Không chỉ báo accuracy tổng.
- Lưu model version, seed, split, tokenizer, normalization, label order, threshold và metrics. Save/load round-trip phải giữ dự đoán trong tolerance phù hợp.
- Dùng CPU trước. Khóa cùng phiên bản DL4J/ND4J; smoke test native backend trước training thật. Spark và Java Maven có môi trường dependency riêng.
- Không tuyên bố số Video bảo đảm mô hình tốt; baseline có 145 Video thiếu caption/hashtag. Kiểm tra số mẫu có nhãn, độ phủ theo thời gian và chất lượng test thực tế.

## 7. Thống kê và Power BI

- Mỗi bảng có grain, khóa, kiểu dữ liệu, timezone, null policy và phiên bản schema.
- Đếm Video duy nhất trong từng Topic; tổng các Topic nhiều nhãn không phải tổng Video duy nhất.
- Bảng view/like theo tuần đăng phải ghi rõ đó là snapshot hiện tại của video đăng tuần đó, không phải lượt tương tác phát sinh trong tuần.
- Chỉ tính tốc độ tương tác từ ít nhất hai bản ghi quan sát nguồn có timestamp phù hợp. Báo khoảng thời gian và xử lý delta âm như dữ liệu cần giải thích, không tự xóa dấu.
- Phân biệt tuần thiếu độ phủ với tuần thật sự không có bài; không tự điền 0 ở vùng chưa quan sát. Đánh dấu tuần chưa đủ và mẫu quá ít.
- PageRank/cộng đồng phụ thuộc projection và window. Không gọi PageRank là tốc độ tăng hoặc bằng chứng lan truyền.
- CSV UTF-8 với cột/kiểu nhất quán; ghi output hoàn chỉnh bằng staging/atomic replacement khi phù hợp, tránh Power BI đọc file đang ghi.
- Nếu có API, dùng Spring Boot phục vụ kết quả đã tính. Không train hoặc khởi chạy job nặng trong request refresh.
- Thay đổi schema đầu ra phải cập nhật hợp đồng và hướng dẫn refresh. Không chỉnh sửa `.pbix` bằng thao tác nhị phân.

## 8. Docker và streaming

- HDFS thuộc thiết kế chính cho yêu cầu lưu trữ phân tán. Không coi thư mục local, Docker volume đơn hoặc Parquet là bằng chứng storage phân tán.
- Tách volume metadata NameNode và từng DataNode; không cho nhiều DataNode dùng chung block directory. Pin Hadoop image/runtime tương thích sau khi smoke test, không giả định JDK Spark phù hợp mọi image Hadoop.
- HDFS client config và URI truy cập phải có ở driver/executor. Địa chỉ DataNode trả về phải truy cập được từ mọi client liên quan. Không chỉ kiểm tra kết nối NameNode mà kết luận toàn bộ read/write hoạt động.
- Config phân biệt `local` và `distributed-demo`; không hardcode `file://`/`local[*]` trong nhánh cần dùng HDFS/Standalone. Bronze bất biến, Silver/Gold theo schema/version, publication có staging và kiểm tra trước khi downstream đọc.
- Driver chia stage và lập lịch task; Master cấp tài nguyên executor. Worker quản lý executor. HDFS block, Spark partition và partition thư mục Parquet là ba khái niệm khác nhau.
- Với PySpark Standalone dùng `spark-submit` client mode từ driver container; không tự chọn Standalone cluster deploy mode cho Python application. Cố định/kiểm tra driver hostname và port khi cần để worker kết nối ngược.
- Điểm xuất phát: 2 core/worker, worker memory 2 GiB, driver heap khoảng 1 GiB; phải kiểm tra RAM/CPU thực và overhead trước chạy. Không bật mọi dịch vụ nặng cùng lúc nếu tài nguyên không đủ.
- Thiết kế task/partition dựa trên dữ liệu. JSON multiLine có thể giới hạn input splitting; kiểm tra execution plan/task, chuyển sang JSONL/Parquet khi cần. Tránh tạo một file/Video và `coalesce(1)` mặc định trên toàn pipeline.
- Bronze/Silver/Gold đặt trên HDFS trong nhánh distributed. DL4J trên Windows nhận export feature có manifest; không coi đường dẫn HDFS trong Docker tự truy cập được từ Windows Java.
- DL4J train CPU một JVM trong bản đầu, Neo4j Community một instance. Không mô tả hai thành phần này là distributed chỉ vì HDFS/Spark chạy cụm.
- Cấu hình theo root project, tránh hardcode đường dẫn cá nhân. Windows mount root vào `/workspace` trong Spark.
- Compose đặt trong `infra/`: kiểm tra đường dẫn mount được resolve từ vị trí Compose. Địa chỉ nội bộ giữa container dùng service name; địa chỉ từ Windows dùng cổng đã publish.
- Pin image version đã kiểm tra; không dùng `latest` làm cấu hình tái lập. Ghi tương thích GDS/Neo4j và package Spark/Kafka connector.
- Neo4j dùng persistent volume. Kafka một broker KRaft, replication factor 1 cho local demo; mô tả đúng không có HA.
- Bí mật lấy từ biến môi trường, không hardcode password/token. Chỉ commit `.env.example` với giá trị mẫu.
- Spark Structured Streaming có checkpoint riêng theo query. Schema/config/state thay đổi phải xét tương thích checkpoint.
- Key Kafka theo Video để giữ thứ tự trong partition; không giả định toàn bộ record có thứ tự thời gian toàn cục.
- Replay dữ liệu lịch sử giữ timestamp nguồn và thêm replay timestamp. Không tạo lần quan sát nguồn mới chỉ vì phát lại record cũ; không gọi replay là crawl realtime.
- `foreachBatch` có thể xử lý lại: sink Neo4j cần upsert/transaction, không chỉ dựa vào checkpoint để hứa exactly-once.
- Không reset database, xóa volume/checkpoint hoặc ghi đè mô hình đang dùng trong lệnh chạy thông thường. Chỉ làm khi người dùng yêu cầu đúng phạm vi và có cách phục hồi phù hợp.
- Không format NameNode như thao tác khởi động thường lệ. Chỉ format khi khởi tạo volume mới đúng phạm vi; startup phải giữ namespace/metadata cũ.
- Replication không thay backup. NameNode đơn và các container cùng host không có HA của host. Mô tả đúng demo process/container, tách biệt multi-host.
- Kiểm chứng HDFS bằng report, fsck block/locations và read-back checksum/record count. Thử dừng DataNode trên cụm thử riêng sau khi xác nhận replica; không làm demo phá lỗi trên dữ liệu chính đang dùng.
- Kiểm chứng Spark bằng event log/UI: có task trên cả hai worker, số executor/core/partition và row count. Không coi hai worker đăng ký nhưng chỉ một worker xử lý là bằng chứng job song song trên hai worker.
- Benchmark cùng dataset/job/storage, mô tả cold/warm, tổng core và memory, số lần đo, variance, shuffle và output equivalence. Dataset nhỏ có thể chạy chậm hơn local; không sửa/bịa số đo để tạo speedup.
- Synthetic load và replay phải tách khỏi video độc lập; không dùng bản nhân đôi làm bằng chứng thêm training sample hoặc xu hướng thật.

## 9. Quy ước triển khai

- Python: module/hàm rõ nhiệm vụ, schema Spark tường minh, type hints cho phần điều phối. Dùng Spark built-in trước Python UDF; tránh collect toàn bộ dữ liệu về driver khi có phép xử lý phân tán phù hợp.
- Java: package đề xuất `vn.bigdata.topic`; Maven Wrapper, config đường dẫn, dependency đồng bộ. Tách train/evaluate/predict.
- Loader Neo4j dùng truy vấn có parameter và batch transaction; không ghép caption vào Cypher bằng chuỗi.
- Log số liệu tổng hợp, run ID và lỗi có nguồn; không in secret hoặc dump toàn bộ nội dung người dùng không cần thiết.
- UTF-8, timezone tường minh, seed và cấu hình có thể tái lập. Không thêm dependency nếu thư viện hiện có đáp ứng.
- Output sinh ra không dùng làm vị trí chứa source code. Việc sửa mapping/taxonomy phải có version hoặc lý do thay đổi.
- Cập nhật README cùng thay đổi cách chạy. Chỉ ghi lệnh đã có entry point/config tương ứng; phân biệt đã kiểm tra local, fixture, dữ liệu thật và chưa kiểm tra.

## 10. Kiểm tra phù hợp từng mốc

| Phần thay đổi | Kiểm tra cần làm |
|---|---|
| Intake/normalize | ID dài, schema biến thể, null/0/false, timestamp, duplicate/conflict, JSON array và JSONL nếu hỗ trợ |
| Đồ thị | 6 label/khóa toàn cục giữa PACK; nhiều Topic/Video, nhiều Topic/Sound; thiếu caption/nhạc; count, projection/liên thông; reload và cập nhật active labels |
| Quan hệ tổng hợp | 6 mẫu/label hai đầu; tag/effect trùng, snapshot lặp; COUNT DISTINCT Video, cửa sổ/timezone/version; không self-loop/cạnh đối xứng; rerun không cộng dồn và bỏ cạnh hết hiệu lực |
| ML | Split không rò rỉ, label order, masked unknown nếu dùng, ND4J smoke test, save/load, fixture nhiều nhãn |
| Báo cáo | Grain, distinct count, tuần giao năm/timezone, thiếu độ phủ, tổng nhiều nhãn và snapshot metrics |
| Streaming | Restart/replay, checkpoint, idempotent sink, record lỗi, timestamp nguồn |
| HDFS | DataNode sống, block locations/replica, volume độc lập, read-back và không format lại khi restart |
| Spark cluster | Client driver nối Master, task trên hai worker, config core/RAM, HDFS read/write, output khớp local |
| Benchmark | Dataset/config có manifest, resource budget, cold/warm, số lần đo và output tương đương |
| API nếu có | Hợp đồng JSON, null, filter/pagination khi cần, đọc kết quả không chạy training |

Kiểm tra hành vi và rủi ro thật; không tạo test chỉ lặp lại implementation. Fixture nhỏ, synthetic và không chứa secret. Chạy kiểm tra phù hợp phần thay đổi; nếu thiếu Docker hoặc dữ liệu thì báo rõ, không giả vờ đã chạy tích hợp.

## 11. Hoàn thành và báo cáo lại

Task hoàn thành khi đầu ra yêu cầu tồn tại, số liệu đối soát được, kiểm tra phù hợp đã chạy và tài liệu phản ánh đúng trạng thái.

Khi bàn giao, nêu ngắn gọn: thay đổi gì, cách dùng, kiểm tra đã chạy và giới hạn còn lại. Không báo số liệu chất lượng/model tự tạo. Không coi có file output là bằng chứng toàn bộ pipeline đã chạy đúng.

Task đầu tiên nếu repository mới là G01: kiểm kê dữ liệu thực trong `data/raw/apify/`, tạo manifest/báo cáo schema và thiếu trường. Tiếp tục G02 dựng HDFS/Spark, rồi G03 normalize; kiểm tra logic local trong G03 không thay nghiệm thu cụm. Khi G01–G03 đạt gate, bàn giao M1 với bằng chứng block/replica và executor/task. Không tự crawl lại hoặc mua dịch vụ chỉ vì dataset nhỏ hơn mục tiêu ban đầu.
