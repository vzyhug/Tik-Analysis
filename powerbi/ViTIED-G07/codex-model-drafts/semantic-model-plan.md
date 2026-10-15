# ViTIED G07 semantic model plan

## Sources and refresh

- Use Power BI Desktop's **PostgreSQL database** connector, server `localhost:5433`, database `trend_detect`, schema `vitied`.
- Import only the `vw_*` views in [Power Query drafts](power-query-source-drafts.md). They expose the current completed publication; refresh reads PostgreSQL and does not invoke Spark or DL4J.
- Model status: draft until loaded and checked in Power BI Desktop. Credentials remain in Desktop's local data-source settings and are not stored in this repository.
- A later publication is append-only in PostgreSQL. The loader flips the current publication flag in one transaction; the views then expose the new release on refresh.

## Tables and relationships

Use single-direction, one-to-many relationships from dimensions into facts. Do not relate fact tables to each other.

| Dimension | Fact relationship | Cardinality / direction |
|---|---|---|
| `DimPublishedDate[Date]` | `FactPostPeriod[published_date]`, `FactHashtagPeriod[published_date]`, `FactHashtagPairPeriod[published_date]`, `FactCoverage[published_date]`, all `FactTopic* [published_date]` | 1:* single |
| `DimObservedDate[Date]` | `FactEngagementObservation[observed_date]` | 1:* single |
| `DimMediaType[media_type]` | `FactPostPeriod[media_type]`, `FactCoverage[media_type]`, `FactEngagementObservation[media_type]` | 1:* single |
| `DimHashtag[tag_key]` | `FactHashtagPeriod[tag_key]` | 1:* single |
| `DimPublication[publication_id]` | each fact `[publication_id]` | 1:* single; current view has one release |

For pair co-occurrence, use the two fields `tag_key_1` and `tag_key_2` directly in a table/matrix. Add separate role-playing hashtag dimensions only if users need slicers for both pair endpoints. Avoid a bidirectional relationship.

Create `DimPublishedDate` from min/max `FactPostPeriod[published_date]` and `DimObservedDate` from min/max `FactEngagementObservation[observed_date]`; mark both as date tables. Include calendar year, month, and ISO week fields. These are separate role-playing dimensions so publication-date filters do not silently become observation-date filters.

## Measures and interpretation

The executable measure drafts are in `measures/` and their definitions are summarized in [measure catalog](measure-catalog.md).

- `PostsPublished` sums the Post-period fact's unique Post counts. It is safe for date/media filters and is not derived from hashtag rows.
- Hashtag and pair support sum distinct Post support at their respective grains. Their totals across tags or pairs are associations and may exceed total Posts.
- Observation count is snapshot rows; unique observed Posts is a distinct ID count. Average views is the average observed `view_count`, not an increase in views.
- Coverage rates use non-null observation counts divided by all snapshots or Posts in the coverage fact. Null remains missing.
- Show Topic and Sound availability from `DimPublication[topic_label_status]` and `[sound_status]`. Current `topic_*`/`topic_sound` views are empty; do not display zero Topic volume as a measured finding.

## Security and use

This is a local course/research report over a sampled KOL/KOC dataset. No user-specific access roles are defined; do not claim RLS. Keep the report in Import mode on the local PostgreSQL instance until a deployment and credential policy is chosen.
