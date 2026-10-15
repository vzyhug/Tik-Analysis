# Power BI Measure Draft

Target table: `FactCoverage`
Measure: `ViewMetricCoverage`

## TMDL Draft

```tmdl
table FactCoverage
    measure ViewMetricCoverage = ```
DIVIDE(SUM(FactCoverage[view_observation_count]), SUM(FactCoverage[observation_count]))
    ```
        formatString: 0.0%
        description: "Share of observation snapshots with non-null view_count; null values remain missing."
```

## Validation Queries

```DAX
EVALUATE ROW("ViewMetricCoverage", [ViewMetricCoverage])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([ViewMetricCoverage]), "Value", [ViewMetricCoverage])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

