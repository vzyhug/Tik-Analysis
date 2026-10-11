# Hướng dẫn rà soát Topic v0 (bản nháp)

Danh sách Topic dự kiến ở `topics_v0_draft.csv`. Đây là **chủ đề của Video**, không phải 24 danh mục KOL. Một Video có thể nhận nhiều `topic_id`; không dùng danh mục KOL, nhạc nền, lượng tương tác hay username làm bằng chứng duy nhất.

## Cách ghi nhãn

1. Mở `data/labels/review_queue.csv` bằng công cụ giữ nguyên `video_id` dạng chuỗi và UTF-8. Mỗi dòng có caption, hashtag, `source_file` và danh mục KOL chỉ để tìm mẫu.
2. Đọc caption và hashtag; nếu cần, kiểm tra lại Video theo nguồn. Chọn mọi Topic có bằng chứng rõ theo cột `include_when`/`exclude_when`.
3. Điền `label_ids` bằng ID Topic ngăn cách ` | `, ví dụ `T01 | T10`. Điền `review_status=reviewed`, `reviewer` và `reviewed_at_utc` dạng ISO 8601 UTC.
4. Nếu không đủ nội dung hoặc còn phân vân, điền `review_status=uncertain` và để `label_ids` trống. Dòng `unreviewed` và `uncertain` **không phải negative** cho bất kỳ Topic nào.
5. Video được rà soát đầy đủ nhưng không thuộc Topic nào trong taxonomy cần trạng thái riêng `reviewed_none` và ghi lý do trong `review_note`; không tự để trống và coi là negative.

`topics_v0_draft.csv` cần được người thực hiện duyệt trước khi dùng để train/test. Bản nháp này không phải ground truth. Sau khi chốt, giữ phiên bản taxonomy và cố định thứ tự `topic_id` cho dữ liệu học máy.
