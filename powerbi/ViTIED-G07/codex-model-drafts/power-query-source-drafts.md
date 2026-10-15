# Power Query sources — draft

**Connector:** PostgreSQL database. **Server:** `localhost:5433`. **Database:** `trend_detect`. **Schema:** `vitied`.

Use Import mode against current-release views. Configure credentials only in Power BI Desktop; no password belongs in this project. The SQL views expose the active publication without starting Spark.

For each fact view, use this pattern in Advanced Editor and replace the `Item` name with the exact view:

```powerquery
let
    Source = PostgreSQL.Database("localhost:5433", "trend_detect", [CreateNavigationProperties = false]),
    Vitied = Source{[Schema = "vitied", Item = "vw_post_period"]}[Data]
in
    Vitied
```

Load these views, then rename tables to the semantic names in `semantic-model-plan.md`:

| PostgreSQL view | Power BI table | Grain |
|---|---|---|
| `vw_post_period` | `FactPostPeriod` | publication date × media type |
| `vw_hashtag_period` | `FactHashtagPeriod` | publication date × hashtag |
| `vw_hashtag_pair_period` | `FactHashtagPairPeriod` | publication date × unordered hashtag pair |
| `vw_engagement_observation` | `FactEngagementObservation` | Post × observation timestamp |
| `vw_coverage` | `FactCoverage` | publication date × media type |
| `vw_topic_period` | `FactTopicPeriod` | publication date × reviewed Topic; currently empty |
| `vw_topic_hashtag` | `FactTopicHashtag` | publication date × reviewed Topic × hashtag; currently empty |
| `vw_topic_sound` | `FactTopicSound` | publication date × reviewed Topic × verified Sound; currently empty |
| `vw_publication` | `DimPublication` | one current publication |

Keep TikTok `post_id` as Text wherever it is used. Keep all count metrics nullable; Power Query must not replace null with zero. Set both date dimensions to Date type and metrics to Whole Number/Decimal as appropriate.
