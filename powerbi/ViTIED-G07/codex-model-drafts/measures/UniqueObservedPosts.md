# Power BI Measure Draft

Target table: `FactEngagementObservation`
Measure: `UniqueObservedPosts`

## TMDL Draft

```tmdl
table FactEngagementObservation
    measure UniqueObservedPosts = ```
DISTINCTCOUNT(FactEngagementObservation[post_id])
    ```
        formatString: #,0
        description: "Distinct Post IDs represented in the filtered observation snapshots."
```

## Validation Queries

```DAX
EVALUATE ROW("UniqueObservedPosts", [UniqueObservedPosts])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([UniqueObservedPosts]), "Value", [UniqueObservedPosts])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

