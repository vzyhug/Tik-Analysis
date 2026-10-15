# DAX measure catalog — reviewed draft

| Measure | Table | DAX | Format | Meaning / guardrail |
|---|---|---|---|---|
| `PostsPublished` | FactPostPeriod | `SUM(FactPostPeriod[post_count])` | `#,0` | Unique Posts by publication date and media type. |
| `HashtagPostSupport` | FactHashtagPeriod | `SUM(FactHashtagPeriod[post_count])` | `#,0` | Unique Post support for hashtags; summing across tags counts associations. |
| `HashtagPairSupport` | FactHashtagPairPeriod | `SUM(FactHashtagPairPeriod[post_count])` | `#,0` | Unique Post support for the selected unordered pair; summing across pairs counts pair associations. |
| `ObservationSnapshots` | FactEngagementObservation | `COUNTROWS(FactEngagementObservation)` | `#,0` | Post × observed timestamp rows, not new engagements. |
| `UniqueObservedPosts` | FactEngagementObservation | `DISTINCTCOUNT(FactEngagementObservation[post_id])` | `#,0` | Unique Posts in the selected observation-date context. |
| `AverageObservedViews` | FactEngagementObservation | `AVERAGE(FactEngagementObservation[view_count])` | `#,0` | Mean view snapshot; never label as views gained or growth. |
| `ViewMetricCoverage` | FactCoverage | `DIVIDE(SUM(FactCoverage[view_observation_count]), SUM(FactCoverage[observation_count]))` | `0.0%` | Non-null view snapshots / all snapshots. |
| `CaptionCoverage` | FactCoverage | `DIVIDE(SUM(FactCoverage[posts_with_caption_first_observation]), SUM(FactCoverage[post_count]))` | `0.0%` | Posts with a non-empty caption at earliest observation / Posts. |
| `TopicLabelStatus` | DimPublication | `IF(SELECTEDVALUE(DimPublication[topic_label_status]) = "unavailable_no_reviewed_post_labels", "No reviewed Post labels", "Review label provenance")` | Text | Makes missing labels visible; no synthetic Topic values. |
| `SoundIdStatus` | DimPublication | `IF(SELECTEDVALUE(DimPublication[sound_status]) = "unavailable_no_verified_sound_ids", "No verified Sound IDs", "Review Sound provenance")` | Text | Makes missing source IDs visible; does not infer identity from sound title. |

The metric owner remains unassigned; the measure drafts label it `Project owner (unassigned)` for later review. This is a local research model, not an operational KPI scorecard.
