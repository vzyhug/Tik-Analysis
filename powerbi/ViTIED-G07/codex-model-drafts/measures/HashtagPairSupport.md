# Power BI Measure Draft

Target table: `FactHashtagPairPeriod`
Measure: `HashtagPairSupport`

## TMDL Draft

```tmdl
table FactHashtagPairPeriod
    measure HashtagPairSupport = ```
SUM(FactHashtagPairPeriod[post_count])
    ```
        formatString: #,0
        description: "Distinct Post support for the selected hashtag pair and date; summing across pairs is not distinct Posts."
```

## Validation Queries

```DAX
EVALUATE ROW("HashtagPairSupport", [HashtagPairSupport])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([HashtagPairSupport]), "Value", [HashtagPairSupport])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

