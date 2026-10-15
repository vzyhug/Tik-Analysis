# Power BI Measure Draft

Target table: `DimPublication`
Measure: `SoundIdStatus`

## TMDL Draft

```tmdl
table DimPublication
    measure SoundIdStatus = ```
IF(SELECTEDVALUE(DimPublication[sound_status]) = "unavailable_no_verified_sound_ids", "No verified Sound IDs", "Review Sound provenance")
    ```
        description: "Human-readable status from the current G06 publication; no sound popularity is inferred from title."
```

## Validation Queries

```DAX
EVALUATE ROW("SoundIdStatus", [SoundIdStatus])
```

```DAX
EVALUATE ROW("IsBlank", ISBLANK([SoundIdStatus]), "Value", [SoundIdStatus])
```

Rollback: remove the measure from PBIP/TMDL or revert the source-control change.

