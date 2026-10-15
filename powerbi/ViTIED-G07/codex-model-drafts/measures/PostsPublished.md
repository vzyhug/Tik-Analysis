# Power BI Measure Draft

Target table: `FactPostPeriod`
Measure: `PostsPublished`

## TMDL Draft

```tmdl
table FactPostPeriod
    measure PostsPublished = ```
SUM(FactPostPeriod[post_count])
    ```
        formatString: #,0
        description: "Distinct Post count by publication date and media_type in the current publication. It does not sum across Topic labels."
```

## Validation Queries

```DAX
EVALUATE ROW("PostsPublished", [PostsPublished])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([PostsPublished]), "Value", [PostsPublished])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

