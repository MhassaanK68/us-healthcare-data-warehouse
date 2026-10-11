# Databricks notebook source
"""Register an isolated two-row Provider Information drift fixture.

Run 00_setup_tables.py with schema=phase2_demo first. This notebook writes a
small synthetic CSV to that schema's landing Volume and registers its exact
bytes in source_file_manifest. It never writes to carewatch.pipeline.
"""

# COMMAND ----------

import csv
import hashlib
import io
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _add_project_src() -> None:
    cwd = Path.cwd()
    for candidate in (cwd / "src", cwd.parent / "src"):
        if (candidate / "carewatch").is_dir():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)
            return
    raise RuntimeError("Could not locate src/carewatch from the Databricks Git folder")


_add_project_src()

from carewatch.audit import MANIFEST_SCHEMA  # noqa: E402
from carewatch.config import DATASETS, qualified_name  # noqa: E402
from carewatch.schemas import source_columns  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "phase2_demo")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
volume = dbutils.widgets.get("volume").strip()
qualified_name(catalog, schema_name, volume)
if schema_name.lower() == "pipeline":
    raise ValueError("Use a separate demo schema, not the production pipeline schema")

manifest_table = qualified_name(catalog, schema_name, "source_file_manifest")
if not spark.catalog.tableExists(manifest_table):
    raise RuntimeError(
        f"Run 00_setup_tables.py with schema={schema_name} first: {manifest_table} is missing"
    )

spark.conf.set("spark.sql.session.timeZone", "UTC")
now = datetime.now(timezone.utc)
acquisition_run_id = str(uuid.uuid4())
batch_id = "demo_" + uuid.uuid4().hex[:24]
dataset = "provider_info"
source_fields = list(source_columns(dataset))
added_column = "phase2_demo_added_column"
good_ccn = "Z" + uuid.uuid4().hex[:5].upper()
bad_ccn = "Z" + uuid.uuid4().hex[:5].upper()
while bad_ccn == good_ccn:
    bad_ccn = "Z" + uuid.uuid4().hex[:5].upper()

good = {column: "" for column in source_fields}
good.update(
    {
        "cms_certification_number_ccn": good_ccn,
        "provider_name": "Synthetic Drift Demo Good Row",
        "state": "CA",
        "zip_code": "00001",
        "overall_rating": "4",
        "processing_date": "2026-09-30",
    }
)
bad = {column: "" for column in source_fields}
bad.update(
    {
        "cms_certification_number_ccn": bad_ccn,
        "provider_name": "Synthetic Drift Demo Bad Rating",
        "state": "CA",
        "zip_code": "00001",
        "overall_rating": "not_an_integer",
        "processing_date": "2026-09-30",
    }
)

buffer = io.StringIO(newline="")
writer = csv.DictWriter(buffer, fieldnames=[*source_fields, added_column])
writer.writeheader()
writer.writerow({**good, added_column: "new source field"})
writer.writerow({**bad, added_column: "new source field"})
content = buffer.getvalue().encode("utf-8")
content_sha256 = hashlib.sha256(content).hexdigest()

landing_path = Path(
    f"/Volumes/{catalog}/{schema_name}/{volume}/demo/provider_info/{acquisition_run_id}.csv"
)
landing_path.parent.mkdir(parents=True, exist_ok=True)
landing_path.write_bytes(content)

manifest_record = {
    "acquisition_run_id": acquisition_run_id,
    "dataset": dataset,
    "dataset_id": DATASETS[dataset].dataset_id,
    "load_type": "full",
    "as_of_date": now.date(),
    "acquisition_strategy": "bulk_snapshot",
    "source_url": f"demo://phase2-drift/{acquisition_run_id}",
    "source_catalog_modified": None,
    "window_start": None,
    "window_end": None,
    "page_offset": None,
    "landing_path": str(landing_path),
    "batch_id": batch_id,
    "source_content_sha256": content_sha256,
    "source_file_sha256": content_sha256,
    "source_bytes": len(content),
    "expected_run_rows": 2,
    "source_rows": None,
    "row_count_validated": False,
    "status": "SUCCESS",
    "error_message": None,
    "load_timestamp": now,
}

spark.createDataFrame([manifest_record], schema=MANIFEST_SCHEMA).write.format(
    "delta"
).mode("append").saveAsTable(manifest_table)

print("Synthetic drift fixture registered in the separate demo schema.")
print(f"acquisition_run_id={acquisition_run_id}")
print(f"batch_id={batch_id}")
print(f"good_ccn={good_ccn}; bad_ccn={bad_ccn}")
print(f"landing_path={landing_path}")
print("Next: run 02_raw_to_bronze.py in this schema for this acquisition_run_id.")
print("Then run 03_bronze_to_silver.py for this batch_id with the secret-name widgets.")

display(
    spark.table(manifest_table)
    .filter(f"acquisition_run_id = '{acquisition_run_id}'")
    .select(
        "dataset",
        "acquisition_run_id",
        "batch_id",
        "landing_path",
        "source_file_sha256",
        "source_bytes",
        "expected_run_rows",
        "status",
    )
)
