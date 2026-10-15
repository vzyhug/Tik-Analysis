# Power BI Measure Draft

Target table: `FactCoverage`
Measure: `CaptionCoverage`

## TMDL Draft

```tmdl
table FactCoverage
    measure CaptionCoverage = ```
DIVIDE(SUM(FactCoverage[posts_with_caption_first_observation]), SUM(FactCoverage[post_count]))
    ```
        formatString: 0.0%
        description: "Share of distinct Posts whose earliest-observation caption is non-empty."
```

## Validation Queries

```DAX
EVALUATE ROW("CaptionCoverage", [CaptionCoverage])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([CaptionCoverage]), "Value", [CaptionCoverage])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

