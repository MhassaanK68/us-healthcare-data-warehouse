"""Refresh the README's column-level Bronze and Silver model tables.

Run from the repository root with ``python scripts/refresh_readme_models.py``.
The local contract test supplies a minimal PySpark types stub when PySpark is
unavailable; the source of truth remains ``src/carewatch/schemas.py``.
"""

from __future__ import annotations

import runpy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
runpy.run_path(str(ROOT / "tests" / "test_step6_silver_contracts.py"))

from carewatch.config import DATASETS  # noqa: E402
from carewatch import schemas  # noqa: E402


START = "<!-- BEGIN DATA MODELS -->"
END = "<!-- END DATA MODELS -->"


def render() -> str:
    lines = [
        START,
        "",
        "## Bronze and Silver data models",
        "",
        "These column lists are generated from the explicit `StructType` contracts in "
        "`src/carewatch/schemas.py`. Run `python scripts/refresh_readme_models.py` "
        "after a deliberate schema change.",
        "",
        "Bronze stores every CMS source field as a nullable `STRING`; its seven "
        "ingestion fields have the types shown below. Bronze retains source duplicates "
        "and therefore has **no enforced row primary key**. `_batch_id` identifies a "
        "replayable source batch, while the listed business-key columns identify "
        "candidate entities within a validated source snapshot. Silver has one "
        "deterministic SHA-256 primary key per table and applies Delta `MERGE` on it.",
        "",
    ]
    for dataset, config in DATASETS.items():
        bronze_schema = schemas.BRONZE_SCHEMAS[dataset]
        lines.extend(
            [
                f"### `{config.bronze_table}` (Bronze)",
                "",
                "Candidate business key: "
                + ", ".join(f"`{key}`" for key in config.key_cols)
                + ". No enforced primary key.",
                "",
                "| Column | Type | Nullable | Role |",
                "|---|---|---|---|",
            ]
        )
        for field in bronze_schema.fields:
            role = "Business key candidate" if field.name in config.key_cols else (
                "Batch lineage" if field.name == "_batch_id" else ""
            )
            lines.append(
                f"| `{field.name}` | `{field.dataType.simpleString().upper()}` | "
                f"{'Yes' if field.nullable else 'No'} | {role} |"
            )
        lines.append("")

    for dataset, config in DATASETS.items():
        silver_schema = schemas.SILVER_SCHEMAS[dataset]
        entity_key = schemas.SILVER_ENTITY_KEYS[dataset]
        lines.extend(
            [
                f"### `{config.silver_table}` (Silver)",
                "",
                f"Primary key: `{entity_key}` (SHA-256 of the ordered business key: "
                + ", ".join(f"`{key}`" for key in config.key_cols)
                + ").",
                "",
                "| Column | Type | Nullable | Key |",
                "|---|---|---|---|",
            ]
        )
        for field in silver_schema.fields:
            key = "PK" if field.name == entity_key else (
                f"BK{config.key_cols.index(field.name) + 1}"
                if field.name in config.key_cols else ""
            )
            lines.append(
                f"| `{field.name}` | `{field.dataType.simpleString().upper()}` | "
                f"{'Yes' if field.nullable else 'No'} | {key} |"
            )
        lines.append("")
    lines.append(END)
    return "\n".join(lines)


def main() -> None:
    readme = ROOT / "README.md"
    text = readme.read_text(encoding="utf-8")
    if START not in text or END not in text:
        raise RuntimeError("README model markers are missing")
    before, remainder = text.split(START, 1)
    _, after = remainder.split(END, 1)
    readme.write_text(before + render() + after, encoding="utf-8")


if __name__ == "__main__":
    main()
