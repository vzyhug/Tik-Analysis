# Báo cáo G07 — Dashboard web đọc Neo4j

**Trạng thái: `in_progress` chờ xem trực quan desktop/màn hẹp.** Dashboard web hiện đọc Neo4j trực tiếp làm DB duy nhất. PostgreSQL G07 trước đây là một bản Gold serving trùng lặp; service đã dừng, volume giữ nguyên để bảo toàn dữ liệu lịch sử. Không xóa volume.

## Nối Observation vào graph

G04 v1 chỉ có Post/Hashtag nên thiếu snapshot để phân tích tương tác. Graph v2 được xuất từ Silver đã kiểm chứng bằng Spark trên cả hai worker và nạp idempotent:

- 9.114 `Post`, 10.549 `Hashtag`, 40.624 `HAS_HASHTAG`.
- 34.750 `Observation`, 34.750 `HAS_OBSERVATION`; mỗi node giữ observed/published time, caption, metric counts và source pointer.
- Export JSON được kiểm checksum và read-back; lần nạp thứ nhất và reload có count như nhau. Chi tiết ở [G04 graph v2](outputs/quality/g04_graph_load.json).

Neo4j là nguồn cho `/api/meta`, `/api/dashboard` và `/api/health`. Dashboard không dùng PostgreSQL, không chạy Spark/training khi tải/làm mới. Graph query theo `datasetId='vitied'`; cache trong app tối đa 20 giây. Credential Neo4j đọc từ Docker secret, không chuyển tới trình duyệt; API chỉ phát truy vấn đọc và cổng bind `127.0.0.1:8787`. Neo4j Community không có role read-only, nên credential ứng dụng vẫn có quyền admin; đây là giới hạn bảo mật cần tính đến nếu triển khai ngoài máy demo.

## Phân tích DL4J caption-only

Model trước dùng caption + hashtag nên không được dùng tiếp để diễn giải nội dung. Feature mới `vitied-g05-caption-idf512-v1` bỏ hashtag hoàn toàn, giữ nguyên split cũ theo author và nhóm caption trùng:

- Train/validation/test giữ nguyên 6.597 / 1.095 / 1.422 Post; số có caption vector dùng được lần lượt 5.947 / 1.023 / 1.284.
- 8.254 Post có caption được gán cluster; 860 abstain (781 thiếu caption ở observation đầu, 79 vector rỗng).
- DL4J autoencoder 64 chiều train 3 vòng trên `spark-worker-1` và `spark-worker-2`; log trực tiếp: [console](outputs/quality/g05_train_vitied-g05-captiononly-ae64-live-20261016.console.txt), report: [JSON](outputs/quality/g05_train_vitied-g05-captiononly-ae64-live-20261016.json).
- Test MSE DL4J 0,001655 so với SVD 0,001321; DL4J tái tạo feature kém hơn. ARI giữa seed 0,982 so với 0,753; silhouette validation 0,078 so với 0,060. Silhouette vẫn thấp, nên các cluster chỉ là cohort để rà soát, không phải Topic được duyệt.
- Kết quả mô hình chỉ được dùng phía backend để xây cohort phân tích; màn khách hàng không hiện mã cụm, ID bài, caption bài mẫu hoặc thông số DL4J.

Artifact model được tạo từ release/checksum tương ứng, dùng danh sách loại từ đệm có cả dạng không dấu để giảm keyword nhiễu, rồi mount read-only: [caption-only dashboard analysis v3](outputs/quality/g05_model_dashboard_vitied-g05-captiononly-ae64-live-20261016-v3.json). Đây chỉ lọc từ mô tả; không thay split/training/assignment. Nhãn Topic vẫn chưa được rà soát; không tạo `HAS_TOPIC` từ cluster.

## Nội dung dashboard

### Dự báo và tín hiệu xu hướng

Đây là phần đầu tiên của trang vì nó trả lời câu hỏi “nhóm nội dung nào đang lên?”. Các tín hiệu tính trên tổng Post của mẫu (bài không có vector nằm ở nhóm “chưa nhận diện”): thay đổi tỷ trọng 4 tuần gần nhất so với 4 tuần trước, số creator hoạt động, số tuần tăng thị phần và kịch bản tỷ trọng 4 tuần. Backend kiểm định damped linear trend trên 8 tuần với baseline giữ nguyên tỷ trọng 4 tuần gần nhất; giao diện khách hàng chỉ trình bày baseline được chọn.

Backtest rolling-origin theo thứ tự thời gian có 7 cửa sổ, mỗi cửa sổ dự báo 4 tuần. MAE của ngoại suy xu hướng là 3,06 điểm phần trăm; baseline là 2,91 điểm, nên dashboard dùng baseline trong biểu đồ và bảng kịch bản. Backtest chưa chứng minh trend model có skill. Đây chỉ là kiểm tra thăm dò có điều kiện: cụm G05 được học trên split tác giả trải khắp thời gian, nên backtest này không phải kiểm định out-of-time nghiêm ngặt của bước khám phá Topic. Nguồn quan sát cuối là 02/07/2025 (khoảng 15 tháng trước ngày chạy dashboard 16/10/2026); các tuần 30/06–21/07/2025 là phép chiếu hồi cứu sau sample cutoff, **không đại diện dự báo xu hướng TikTok hiện tại**. Dữ liệu là mẫu creator ViTIED, không phải toàn bộ nền tảng. Nhóm caption chưa được rà soát thành Topic.

- **Thay đổi thị phần:** so 4 tuần cuối với 4 tuần trước theo cohort nội dung.
- **Bối cảnh mẫu:** Post cohort, snapshot, caption coverage, bài theo tuần và view trung bình theo ngày quan sát.
- **Nhịp đăng:** Post theo tuần, media type và thứ trong tuần.
- **Cụm/nội dung:** median view và tỷ lệ like/comment trên view theo cluster; creator theo volume/độ đa dạng cluster; caption phrases nổi bật, sample gần tâm/ngẫu nhiên.
- **Tương tác:** snapshot theo ngày quan sát, latest snapshot mỗi Post trong kỳ và top Post theo view.
- **Độ phủ:** caption/snapshot/metric theo tháng đăng.
- **Topic status:** hiện không có nhãn Post được duyệt.

Dashboard khách hàng có sidebar sáu mục, mỗi mục mở màn báo cáo riêng để không phải cuộn qua toàn bộ dashboard: Tổng quan, Nhịp tăng/giảm, Kịch bản 4 tuần, Phản hồi nội dung, So sánh nhóm và Mạng lưới quan hệ. Biểu đồ ở trang chi tiết được tăng kích thước. Nội dung tập trung vào câu hỏi ra quyết định: nhóm nào tăng/hạ nhiệt, tín hiệu có lan qua nhiều creator không, kịch bản tỷ trọng 4 tuần nếu nhịp gần đây giữ nguyên, và tỷ lệ like/view quan sát giữa các nhóm. Bảng chi tiết cho biết tỷ trọng trước/gần đây, creator tham gia, số tuần tăng và kịch bản. Không còn bộ lọc Video/Ảnh, ID bài, top bài, thứ trong tuần, số lượng snapshot, bảng coverage hay màn chẩn đoán mô hình. Nhãn trình bày là gợi ý caption để đọc nhanh, không phải Topic được duyệt. Tỷ lệ tương tác mô tả snapshot quan sát của cohort, không kết luận tác động nhân quả; snapshot thời điểm cuối khác nhau theo Post.

## Khởi chạy và kiểm tra

```powershell
docker compose -f infra/compose.yaml --profile graph -f infra/compose.web.yaml up -d --build neo4j web-dashboard
python scripts/verify_g07_web_dashboard.py
```

Kiểm tra API/đối soát và nội dung UI hiện hành: [g07_web_dashboard.json](outputs/quality/g07_web_dashboard.json), 40 checks đạt. Mạng hashtag toàn cục lấy đồng xuất hiện từ toàn bộ quan hệ Post–Hashtag; tag phủ trên 5% bài giữ trong thống kê tổng nhưng bỏ khỏi scoring. Cạnh còn lại cần tối thiểu 3 Post và 2 creator, trọng số PPMI có hiệu chỉnh IDF; ArticleRank tính vai trò trung tâm, Closeness Centrality xếp mức gần các hashtag khác bằng khoảng cách đường đi có trọng số. Trên kỳ 06/12/2022–02/07/2025 có 10.549 hashtag, 62.067 cặp ứng viên, 3.451 cạnh được chấm và 58.616 cặp bị loại. #xuhuong xuất hiện 1.571/9.114 Post nên không định nghĩa liên kết; ArticleRank cao nhất là #skincare, closeness centrality score cao nhất là #skincare. Dashboard đã rebuild và chạy trong Docker. Browser surface không được CUA cung cấp nên xem trực quan desktop/màn hẹp còn chờ.

## PostgreSQL archive

Publication `vitied-gold-v1-20261014` và verification [g07_postgres](outputs/quality/g07_postgres_vitied-gold-v1-20261014.json) là evidence của lần triển khai trước. PostgreSQL service đã dừng, không bị xóa container volume; không còn tham gia đường đi request của dashboard.
