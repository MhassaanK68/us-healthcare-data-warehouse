# CareWatch Phase 2 status

Updated: 2026-10-11 (Asia/Karachi)

## Implemented in the repository

- CMS acquisition for four datasets, including full bulk downloads and incremental API or snapshot refreshes.
- Explicit source and Bronze `StructType` schemas, manifest-backed Raw-to-Bronze ingestion, lineage, drift handling, and Bronze quarantine.
- Four typed Silver schemas, validation and Silver quarantine, deterministic keys and hashes, and hash-aware Delta `MERGE`.
- Parameterized batch and ingestion-date replay in the Raw-to-Bronze and Bronze-to-Silver notebooks.
- Per-layer execution logging and a read-only evidence notebook.
- An isolated synthetic drift-fixture notebook and an exact recording walkthrough.
- README column-level Bronze and Silver models and a concrete incremental/backfill run guide.

## Locally verified

- `python -m unittest discover -s tests -q`: 13 schema-contract checks pass.
- All repository Python files and acquisition notebook cells parse.
- Acquisition notebook is valid JSON; executable files have no `inferSchema` call.
- `git diff --check` reports no whitespace errors.

## Live Databricks verification

The read-only queries recorded in `docs/evidence/databricks_validation_2026-10-11.md` confirm populated tables, four unique-key Silver targets, no null `load_timestamp` values in current Bronze/Silver rows, full and incremental execution logs, a no-op `reprocess=true` Silver batch, and 36 Silver quarantine rows. The live schema-drift log is empty.

## Required before submission

The attached rubric also requires saved executed notebook outputs or a short screen recording. The SQL validation report is supporting evidence, but neither those outputs nor a recording is in the repository. Follow `docs/phase2_evidence_walkthrough.md` to capture the controlled drift case, widget-driven backfill, and remaining on-screen demonstration.

The reviewed project was pushed to `https://github.com/MhassaanK68/us-healthcare-data-warehouse`. The remote HEAD matched the pushed commit, and an unauthenticated HTTP request to the repository page returned `200`.
