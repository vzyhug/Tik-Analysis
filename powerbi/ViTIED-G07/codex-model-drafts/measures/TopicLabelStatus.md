# Power BI Measure Draft

Target table: `DimPublication`
Measure: `TopicLabelStatus`

## TMDL Draft

```tmdl
table DimPublication
    measure TopicLabelStatus = ```
IF(SELECTEDVALUE(DimPublication[topic_label_status]) = "unavailable_no_reviewed_post_labels", "No reviewed Post labels", "Review label provenance")
    ```
        description: "Human-readable status from the current G06 publication. Does not turn empty labels into a zero Topic count."
```

## Validation Queries

```DAX
EVALUATE ROW("TopicLabelStatus", [TopicLabelStatus])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([TopicLabelStatus]), "Value", [TopicLabelStatus])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

