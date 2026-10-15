# ViTIED G07 Power BI model draft

> Archive note (2026-10-15): the user selected a local web dashboard instead of Power BI for G07. This design pack is retained as reference only; current implementation and run instructions are in [GD7.md](../../GD7.md).

This directory is a Power BI Model Wizard design pack, **not a PBIP/PBIX report**. It uses the current, locally verified PostgreSQL publication `vitied-gold-v1-20261014`.

## Contents

- `codex-model-drafts/model-design.json`: source, facts, dimensions, and report-page plan.
- `codex-model-drafts/data-contract-draft.json`: table grains, date and null policies, Topic label guardrails.
- `codex-model-drafts/power-query-source-drafts.md`: PostgreSQL Import-mode query pattern and source views.
- `codex-model-drafts/semantic-model-plan.md`: relationship plan and model semantics.
- `codex-model-drafts/measure-catalog.md` and `measures/`: measure definitions and TMDL drafts generated with the Power BI Model Wizard tools.
- `codex-model-drafts/report-page-plan.md`: pages, visuals, filters, and interpretation notes.

## Open the data in Power BI Desktop

1. Choose **Get data → PostgreSQL database** (not the generic ODBC connector).
2. In the connection dialog, enter server `localhost:5433` and database `trend_detect`, choose **Import**, then select **OK/Connect**. This dialog only sets the connection; it does not list tables yet.
3. In the **Navigator** window, expand the `vitied` schema. Select the `vw_*` views listed below, then choose **Load** or **Transform data**. The views appear after the connection succeeds, not on the initial server/database dialog.
4. Apply table names and single-direction relationships in the semantic model plan; add measures from the catalog.
5. Create report pages from the page plan and validate slicer/filter results against the database and G07 verification report.

No credentials are stored here. The report is still a draft until it is created and visually/semantically checked in Power BI Desktop. Topic pages must remain in the explicit unavailable state while reviewed Post labels are absent.

### Views to select in Navigator

Select `vw_post_period`, `vw_hashtag_period`, `vw_hashtag_pair_period`, `vw_engagement_observation`, `vw_coverage`, and `vw_publication` under schema `vitied`. The three `vw_topic_*` views are valid but currently empty because reviewed Post labels and verified Sound IDs are not available; they can be omitted from the first import.

If Navigator is empty, confirm the connection dialog has both server and database filled and you clicked **Connect**; ensure the PostgreSQL container is running and reachable at `localhost:5433`; then expand `vitied` (views may be nested under the schema rather than shown at the top level). The PostgreSQL login needs `USAGE` on schema `vitied` and `SELECT` on its views. The `powerbi_reader` role has now been granted those read-only permissions and verified against all nine views. In Power BI, clear any cached permission for this PostgreSQL source via **File → Options and settings → Data source settings**, reconnect, and authenticate with database credentials for `powerbi_reader`; then refresh Navigator. Do not type `vitied.vw_*` into the database field.
