# Power BI Measure Draft

Target table: `FactHashtagPeriod`
Measure: `HashtagPostSupport`

## TMDL Draft

```tmdl
table FactHashtagPeriod
    measure HashtagPostSupport = ```
SUM(FactHashtagPeriod[post_count])
    ```
        formatString: #,0
        description: "Distinct Post support per date and hashtag; summing across hashtags counts associations and can exceed distinct Posts."
```

## Validation Queries

```DAX
EVALUATE ROW("HashtagPostSupport", [HashtagPostSupport])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([HashtagPostSupport]), "Value", [HashtagPostSupport])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

