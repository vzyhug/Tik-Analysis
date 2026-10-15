# Power BI Measure Draft

Target table: `FactEngagementObservation`
Measure: `ObservationSnapshots`

## TMDL Draft

```tmdl
table FactEngagementObservation
    measure ObservationSnapshots = ```
COUNTROWS(FactEngagementObservation)
    ```
        formatString: #,0
        description: "Count of Post by observed_at_utc records; not new views or distinct Posts."
```

## Validation Queries

```DAX
EVALUATE ROW("ObservationSnapshots", [ObservationSnapshots])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([ObservationSnapshots]), "Value", [ObservationSnapshots])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

