# Power BI report pages — G07

## 1. Overview

- Cards: `PostsPublished`, `ObservationSnapshots`, `AverageObservedViews`, `ViewMetricCoverage`.
- Line: `DimPublishedDate[YearWeek]` × `PostsPublished`, legend `DimMediaType[media_type]`.
- Bar: top 10 `DimHashtag[tag_key]` by `HashtagPostSupport`.
- Slicers: publication date, media type. Footer/card: dataset version, publication id, timezone from `DimPublication`.
- Caption: “ViTIED KOL/KOC sample; counts describe the captured sample, not all TikTok.”

## 2. Hashtag relationships

- Matrix/table: `tag_key_1`, `tag_key_2`, `HashtagPairSupport`; sort descending by support; allow a date slicer.
- Do not sum pair support and label it as total Posts. Do not describe co-occurrence as causal influence or viral spread.

## 3. Engagement snapshots

- Line: `DimObservedDate[YearWeek]` × `AverageObservedViews`; supporting cards `UniqueObservedPosts`, `ObservationSnapshots`, `ViewMetricCoverage`.
- Use observation date, not publication date. State that values are observed snapshots and vary with post age; they are not views gained in the selected period.
- Keep null metric values blank; never fill with zero. No velocity/delta unless a separate G06 release pairs at least two same-Post observations.

## 4. Coverage and Topic availability

- Coverage cards: `CaptionCoverage`, `ViewMetricCoverage`, `TopicLabelStatus`, `SoundIdStatus`.
- Topic/Sound visuals should show an explicit unavailable-state message while the source tables are empty; do not plot an empty chart with a “0 topics” conclusion.
- If later reviewed labels are loaded, show manual labels separately from provisional predictions and creator category. Add label source and support to every Topic visual.

## Interactions / QA

- All pages show current publication and version; refresh should update from current PostgreSQL views only.
- Date slicers must use `DimPublishedDate` for posts/hashtags/coverage and `DimObservedDate` for engagement.
- No interactions between fact tables; single-direction dimensions avoid ambiguous filters.
