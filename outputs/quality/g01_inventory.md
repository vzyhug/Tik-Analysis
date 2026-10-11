# G01 — Kiểm kê nguồn Apify

Nguồn: 8 PACK trong `data/raw/apify/`; checksum SHA-256 trong manifest.
`observed_at` chưa biết; timestamp trong tên file không được dùng làm thời gian quan sát.

| PACK | Records | Video records | Source errors | Profile only | SHA-256 |
|---|---:|---:|---:|---:|---|
| PACK1 | 685 | 680 | 5 | 0 | `b8f00482831f82035f29ddb120a790db986c1d1a80fd4dabde7b972f94ec8745` |
| PACK2 | 666 | 660 | 4 | 2 | `0f5a466efc246e50abf988f98c8528de5a311cbece9b97d265430275803a0cc2` |
| PACK3 | 692 | 690 | 2 | 0 | `6b73e4eb573132e6b6a8c717321ceb6d7717d8d53346ca1c2357b5c1821475e1` |
| PACK4 | 702 | 700 | 2 | 0 | `49db388aa0c5c1ce386ae66c05be87b0ca391edc0d247b29ffa04ee3920f2783` |
| PACK5 | 603 | 595 | 3 | 5 | `e2ea301baabb22c82c2acaa8567ac2453d3111515a13df6239b69a06ed8aac4a` |
| PACK6 | 553 | 536 | 9 | 8 | `0833cd072f9189329505d1c84fb50fc5de72b3e1626d4a91333834592ceb8dc1` |
| PACK7 | 203 | 200 | 3 | 0 | `45aff2a0ae0261403cf0688a414a90e91176f347fcb9cec4e57268b19270c8c5` |
| PACK8 | 621 | 612 | 8 | 1 | `53266e2a85d5a8988c00390aa5cecf053e61fe12d74396bb061bdf4cbcf28e0b` |

## Tổng

- confirmed_empty_hashtags: 908
- empty_or_missing_text: 145
- profile_only: 16
- records: 4725
- source_error: 36
- video_records: 4673
- unique_video_ids: 4673
- unique_author_ids_in_video_records: 475
- unique_author_usernames_in_video_records: 475
- unique_input_usernames_in_all_records: 527
- unique_sound_ids: 4326
- unique_hashtag_keys: 4360
- unique_effect_ids: 288
- unique_location_ids: 210

- published_at_utc: 2020-09-09T06:05:03+00:00 đến 2026-10-11T11:20:21+00:00

## Trường thiếu và null trên video records

| Field | Absent | Null |
|---|---:|---:|
| `text` | 0 | 0 |
| `createTime` | 0 | 0 |
| `createTimeISO` | 0 | 0 |
| `authorMeta.id` | 0 | 0 |
| `authorMeta.name` | 0 | 0 |
| `musicMeta.musicId` | 0 | 0 |
| `hashtags` | 0 | 0 |
| `effectStickers` | 0 | 0 |
| `locationMeta.locationId` | 4281 | 0 |
| `playCount` | 0 | 0 |
| `diggCount` | 0 | 0 |
| `shareCount` | 0 | 0 |
| `commentCount` | 0 | 0 |
| `collectCount` | 0 | 0 |
| `repostCount` | 0 | 0 |
| `isAd` | 0 | 0 |
| `isSponsored` | 0 | 0 |
| `hasTikTokShopProduct` | 0 | 0 |

## Đối soát danh sách KOL

- `kol.csv`: 720 dòng, 578 username, 24 danh mục; SHA-256 `8beab66e5e8b6356864c83c3fc5fde493b3bd7f7f46b213d5b1fa464c5396e6d`.
- 527 username có record nguồn; 475 có Video.
- 51 username không xuất hiện trong record nguồn; 52 có record nhưng không có Video.
- 0 dòng ánh xạ trùng; 0 category ID có tên xung đột.
Danh sách username cần đối soát chi tiết nằm trong manifest JSON. Danh mục KOL là tham khảo, không tự gán thành Topic cho Video.

Chi tiết schema từng PACK, pointer quarantine, duplicate và conflict nằm trong `g01_inventory.json`.
`source_error`/`profile_only` không là Video. Duplicates/conflicts cùng video_id + collection_id được báo, không âm thầm chọn bản cuối.
Dữ liệu này là mẫu KOL đã chọn, không đại diện toàn TikTok.
