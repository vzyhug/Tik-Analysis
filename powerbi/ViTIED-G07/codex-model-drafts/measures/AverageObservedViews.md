# Power BI Measure Draft

Target table: `FactEngagementObservation`
Measure: `AverageObservedViews`

## TMDL Draft

```tmdl
table FactEngagementObservation
    measure AverageObservedViews = ```
AVERAGE(FactEngagementObservation[view_count])
    ```
        formatString: #,0
        description: "Mean view_count per observed snapshot in the current filter context; not a view delta or velocity."
```

## Validation Queries

```DAX
EVALUATE ROW("AverageObservedViews", [AverageObservedViews])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([AverageObservedViews]), "Value", [AverageObservedViews])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

