# Báo cáo G01: kiểm kê và chốt khóa bài đăng ViTIED

**Kết quả:** G01 đã hoàn thành cho hai CSV ViTIED có checksum trong [manifest nguồn](data/raw/manifests/vitied_source_20261013.json). Cả **34.750 dòng nguồn** được đối soát thành **34.750 lần quan sát hợp lệ, 0 dòng quarantine** và **9.114 bài đăng duy nhất**. Trong đó có **8.731 video** và **383 bài ảnh**. Đây là kết quả kiểm kê nguồn và thiết kế khóa; chưa phải Silver trên HDFS hay kết quả mô hình.

Điểm thay đổi quan trọng là `vid_id` của **toàn bộ 5.114 dòng streaming** khác ID nằm trong URL TikTok. ViTIED cũng chứa đường dẫn `/photo/`, nên thực thể chính của pipeline mới là **Post** với `media_type=video/photo`. G01 dùng ID trong URL TikTok hợp lệ làm `canonical_post_id`, giữ `vid_id` gốc để truy vết. Quy tắc này được áp dụng sau khi kiểm tra ánh xạ ID một-một, tác giả và loại bài nhất quán trên hai file. Nó không chứng minh các URL vẫn truy cập được trên TikTok hiện nay.

## 1. Phạm vi và nguồn bằng chứng

| Nguồn | Dòng CSV | Cột | Kích thước | SHA-256 |
|---|---:|---:|---:|---|
| `training_data.csv` | 29.636 | 29 | 16.189.863 byte | `370234d0358b7103e7574dd7718054e82f53ce5092a6024e1ec5ed288b8b0c10` |
| `streaming_data.csv` | 5.114 | 21 | 1.944.254 byte | `fa9fe04c9144050f34375b2cc3cfb4e9edc3a0b15b7f36af7c9476e33e8d21da` |

Hai file được đọc tại `data/raw/vitied/`; script không chỉnh sửa raw. HEAD của repository nguồn được quan sát riêng lúc tải; **SHA-256 ở bảng trên xác định chính xác bytes đã kiểm kê**, không giả định file `main` hiện tại còn giống bản này. Bằng chứng chi tiết: [source audit](outputs/quality/vitied_source_audit.json), [đối soát G01](outputs/quality/vitied_g01.json), [bảng ID](outputs/quality/vitied_id_resolution.csv) và [pointer quarantine](data/quarantine/vitied_g01_pointers.csv).

## 2. Đối soát số lượng và grain

| Phép đối soát | Kết quả | Cách hiểu |
|---|---:|---|
| Dòng nguồn | 34.750 | 29.636 training + 5.114 streaming |
| Lần quan sát được chấp nhận | 34.750 | Grain: `canonical_post_id × observed_at_utc` |
| Dòng quarantine | 0 | Không phát hiện trường hợp vi phạm gate G01 trong bản nguồn này |
| Bài đăng canonical | 9.114 | ID từ URL TikTok hợp lệ; một dòng/bài trong bảng ID |
| Video / bài ảnh | 8.731 / 383 | Phân loại theo `/video/` hoặc `/photo/` trong URL |
| Username phân biệt | 384 | Username nguồn, **không** phải ID người dùng đã xác minh |

Kiểm tra độc lập file xuất: bảng ID có **9.114 khóa duy nhất**, tổng `observation_count` trên bảng ID là **34.750**, và file quarantine có **0 dòng dữ liệu**. Cả hai file nguồn không có ID bài trùng nhau; không có cặp bài–thời điểm quan sát trùng hoặc xung đột. Con số 0 quarantine chỉ áp dụng cho quy tắc kiểm tra G01 và phiên bản nguồn này, không có nghĩa mọi thuộc tính đều đầy đủ hoặc đúng ngoài nguồn.

## 3. Quy tắc khóa, thời gian và nhãn nguồn

### Khóa bài đăng

- Chỉ nhận URL có host TikTok và đường dẫn `/@<username>/video/<id>` hoặc `/@<username>/photo/<id>`; ID là chuỗi 18–20 chữ số. Không ép ID qua `float`.
- Kiểm tra `vid_id` thô ↔ ID URL không ánh xạ một-nhiều, ID URL không gắn với nhiều raw ID, tác giả hoặc loại bài. Kết quả trên nguồn hiện tại: **0 xung đột** ở các kiểm tra này.
- `training_data.csv`: raw ID khớp URL. `streaming_data.csv`: **5.114/5.114 dòng lệch**; sai khác tuyệt đối 257–312 đơn vị. Mẫu sai khác phù hợp dấu hiệu ID đã bị làm tròn trong export, nhưng script không tự khôi phục bằng tính toán; nó chọn ID nguyên văn từ URL và ghi trạng thái `url_selected_raw_mismatch`.
- Bản ghi không có URL TikTok hợp lệ, timestamp không có offset hợp lệ, hoặc có xung đột ánh xạ sẽ đi quarantine kèm `source_file`, số dòng CSV và lý do. Fixture test đã kiểm tra các nhánh này; nguồn thật hiện không phát sinh dòng quarantine.

### Thời gian và snapshot

- `vid_postTime` là thời gian đăng; `vid_scrapeTime` là thời gian quan sát. Cả hai được parse với offset nguồn rồi đưa về UTC; không dùng thời gian import thay thời gian quan sát.
- **1.829 bài** có các giá trị giây đăng khác nhau giữa các snapshot, nhưng không bài nào lệch **phút đăng**. Bảng ID lưu `published_minute_utc`; G03 phải giữ timestamp gốc của từng dòng Observation để truy vết. Khi làm lịch tuần/tháng, dùng timezone `Asia/Ho_Chi_Minh`.
- Khoảng đăng nguồn: `2022-12-06T15:47:14Z` đến `2025-06-28T04:43:43Z`. Khoảng quan sát: `2025-06-23T16:12:05Z` đến `2025-07-01T22:25:21Z`. Các view/like là **snapshot được đọc trong khoảng quan sát**, không phải tương tác phát sinh trong tuần đăng.

### Nhãn và thuộc tính

- Trường `topic` không đổi theo từng username có giá trị trong mỗi file (0/371 tác giả training và 0/347 tác giả streaming đổi giá trị). G01 giữ nó là **`creator_category`** gồm sáu nhóm chuẩn hóa chữ thường; **không** coi là nhãn Topic của từng bài. Có 520 dòng thiếu/unknown category. Nhãn Topic cấp bài và quy trình duyệt thuộc G05.
- `music_id` phần lớn ở ký pháp khoa học: **29.559 dòng training** và **5.108 dòng streaming** không có dạng ID nguyên vẹn theo kiểm tra G01. Không suy ra Sound ID chính xác từ số đã làm tròn hoặc gộp Sound theo tên.
- `vid_nshare` có **848 dòng** mang chữ `Share`; đó là giá trị chưa parse/thiếu, không phải số 0. Các metric khác được kiểm tra định dạng văn bản ở G01; phép chuyển số và null policy đầy đủ sẽ thuộc adapter G03.

## 4. Độ đầy đủ của nguồn

| Trường | Training thiếu | Streaming thiếu | Xử lý tiếp theo |
|---|---:|---:|---|
| Caption | 2.397 | 543 | Giữ bài; đánh dấu thiếu nội dung văn bản |
| Hashtag | 3.728 | 737 | Giữ bài; không tạo cạnh hashtag nếu thiếu |
| `topic`/creator category | 80 | 440 | Giữ unknown; không gán negative hay nhãn Topic |
| `music_id` | 77 | 6 | Tách khỏi Sound ID chưa xác thực |

Các file có schema khác nhau: training có thêm tám cột, gồm bảy cột xử lý sẵn (`vid_desc_clean`, `vid_hashtags_normalized`, `hashtag_count`, `vid_duration_sec`, `vid_existtime_hrs`, `post_hour`, `post_day`) và cột index `Unnamed: 0`. G03 phải đọc theo adapter từng nguồn; không mặc định các cột này có trong streaming hoặc dùng cột có thể chứa thông tin về sau làm feature ML.

## 5. Cách tái lập và kiểm tra

Chạy tại gốc repository, với hai CSV raw đúng checksum manifest:

```powershell
python scripts/audit_vitied.py
python scripts/intake_vitied.py
python -m unittest discover -s tests -p test_vitied_*.py -v
```

`intake_vitied.py` dừng nếu thiếu file, sai SHA-256/kích thước/số dòng hoặc schema bắt buộc; output JSON/CSV được thay atomically sau khi phân tích xong. Lần chạy đã báo **4/4 unit test đạt**: URL video/photo, category, raw ID lệch, ánh xạ ID mơ hồ, URL ngoài TikTok, bản sao/xung đột Observation và quarantine. `git diff --check` không báo lỗi whitespace. Kiểm tra tổng file xuất đã xác nhận 9.114 ID duy nhất và tổng 34.750 observation.

## 6. Kết luận và chuyển giao

**G01 đạt gate trên bản nguồn có SHA-256 nêu ở mục 1.** Hợp đồng đưa sang G02–G03 là `Post` (`post_id`, `media_type`, `published_minute_utc`), `Observation` (`post_id`, `observed_at_utc`, timestamp đăng gốc và metric snapshot), cùng raw ID/source pointer và `creator_category` có provenance. Khi hai CSV thay đổi, phải chạy lại G01 trước khi xuất Silver.

G02 và G03 đã được nghiệm thu tiếp theo trên hai CSV ViTIED; xem [GD2.md](GD2.md) và [GD3.md](GD3.md). Graph, DL4J, Gold và Power BI ViTIED thuộc các gate sau.
