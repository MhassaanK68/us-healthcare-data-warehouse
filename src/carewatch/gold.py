"""Gold dimensional model, idempotent Delta merges, and validation for CareWatch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Sequence

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from carewatch.config import qualified_name


UNKNOWN_STATE_KEY = "__UNKNOWN__"
MDS_PERIOD_PATTERN = r"^[0-9]{4}Q[1-4]-[0-9]{4}Q[1-4]$"


def _technical_fields() -> list[StructField]:
    return [
        StructField("business_hash", StringType(), False),
        StructField("load_timestamp", TimestampType(), False),
    ]


GOLD_SCHEMAS: dict[str, StructType] = {
    "dim_facility": StructType(
        [
            StructField("facility_key", StringType(), False),
            StructField("cms_certification_number_ccn", StringType(), False),
            StructField("provider_name", StringType(), True),
            StructField("ownership_type", StringType(), True),
            StructField("chain_id", StringType(), True),
            StructField("chain_name", StringType(), True),
            StructField("citytown", StringType(), True),
            StructField("current_state_code", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("overall_rating", IntegerType(), True),
            StructField(
                "reported_total_nurse_staffing_hours_per_resident_per_day",
                DoubleType(),
                True,
            ),
            StructField("total_nursing_staff_turnover", DoubleType(), True),
            StructField("number_of_certified_beds", IntegerType(), True),
            StructField("profile_as_of_date", DateType(), True),
            StructField("is_currently_listed", BooleanType(), False),
            *_technical_fields(),
        ]
    ),
    "dim_date": StructType(
        [
            StructField("date_key", IntegerType(), False),
            StructField("calendar_date", DateType(), False),
            StructField("year", IntegerType(), False),
            StructField("quarter", IntegerType(), False),
            StructField("month_number", IntegerType(), False),
            StructField("month_start_date", DateType(), False),
            StructField("year_month", StringType(), False),
            *_technical_fields(),
        ]
    ),
    "dim_geography": StructType(
        [
            StructField("state_key", StringType(), False),
            StructField("state_code", StringType(), True),
            StructField("state_label", StringType(), False),
            *_technical_fields(),
        ]
    ),
    "dim_deficiency": StructType(
        [
            StructField("deficiency_type_key", StringType(), False),
            StructField("deficiency_prefix", StringType(), False),
            StructField("deficiency_tag_number", StringType(), False),
            StructField("deficiency_category", StringType(), True),
            *_technical_fields(),
        ]
    ),
    "dim_measure": StructType(
        [
            StructField("measure_key", StringType(), False),
            StructField("measure_code", StringType(), False),
            StructField("resident_type", StringType(), False),
            StructField("measure_description", StringType(), True),
            *_technical_fields(),
        ]
    ),
    "fact_deficiency": StructType(
        [
            StructField("deficiency_key", StringType(), False),
            StructField("facility_key", StringType(), False),
            StructField("survey_date_key", IntegerType(), False),
            StructField("state_key", StringType(), False),
            StructField("deficiency_type_key", StringType(), False),
            StructField("survey_date", DateType(), False),
            StructField("severity_group", StringType(), True),
            StructField("scope_severity_code", StringType(), True),
            StructField("deficiency_corrected", StringType(), True),
            StructField("correction_date", DateType(), True),
            StructField("citation_count", IntegerType(), False),
            StructField("serious_citation_count", IntegerType(), False),
            StructField("is_deleted", BooleanType(), False),
            StructField("source_batch_id", StringType(), False),
            StructField("source_processing_date", DateType(), True),
            *_technical_fields(),
        ]
    ),
    "fact_penalty": StructType(
        [
            StructField("penalty_key", StringType(), False),
            StructField("facility_key", StringType(), False),
            StructField("penalty_date_key", IntegerType(), False),
            StructField("state_key", StringType(), False),
            StructField("penalty_date", DateType(), False),
            StructField("penalty_type", StringType(), False),
            StructField("fine_amount", DecimalType(14, 2), True),
            StructField("payment_denial_length_in_days", IntegerType(), True),
            StructField("penalty_count", IntegerType(), False),
            StructField("fine_count", IntegerType(), False),
            StructField("payment_denial_count", IntegerType(), False),
            StructField("is_deleted", BooleanType(), False),
            StructField("source_batch_id", StringType(), False),
            StructField("source_processing_date", DateType(), True),
            *_technical_fields(),
        ]
    ),
    "fact_mds_quality": StructType(
        [
            StructField("mds_key", StringType(), False),
            StructField("facility_key", StringType(), False),
            StructField("period_end_date_key", IntegerType(), False),
            StructField("state_key", StringType(), False),
            StructField("measure_key", StringType(), False),
            StructField("measure_period", StringType(), False),
            StructField("period_end_date", DateType(), False),
            StructField("q1_measure_score", DoubleType(), True),
            StructField("footnote_for_q1_measure_score", IntegerType(), True),
            StructField("q2_measure_score", DoubleType(), True),
            StructField("footnote_for_q2_measure_score", IntegerType(), True),
            StructField("q3_measure_score", DoubleType(), True),
            StructField("footnote_for_q3_measure_score", IntegerType(), True),
            StructField("q4_measure_score", DoubleType(), True),
            StructField("footnote_for_q4_measure_score", IntegerType(), True),
            StructField("four_quarter_average_score", DoubleType(), True),
            StructField(
                "footnote_for_four_quarter_average_score", IntegerType(), True
            ),
            StructField("is_deleted", BooleanType(), False),
            StructField("source_batch_id", StringType(), False),
            StructField("source_processing_date", DateType(), True),
            *_technical_fields(),
        ]
    ),
    "agg_state_monthly_compliance": StructType(
        [
            StructField("state_key", StringType(), False),
            StructField("month_date_key", IntegerType(), False),
            StructField("citation_count", LongType(), False),
            StructField("serious_citation_count", LongType(), False),
            StructField("penalty_count", LongType(), False),
            StructField("fine_count", LongType(), False),
            StructField("payment_denial_count", LongType(), False),
            StructField("fine_amount_total", DecimalType(20, 2), False),
            *_technical_fields(),
        ]
    ),
}


GOLD_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "dim_facility": ("facility_key",),
    "dim_date": ("date_key",),
    "dim_geography": ("state_key",),
    "dim_deficiency": ("deficiency_type_key",),
    "dim_measure": ("measure_key",),
    "fact_deficiency": ("deficiency_key",),
    "fact_penalty": ("penalty_key",),
    "fact_mds_quality": ("mds_key",),
    "agg_state_monthly_compliance": ("state_key", "month_date_key"),
}


GOLD_CURRENT_STATE_TABLES = frozenset(
    {
        "dim_facility",
        "fact_deficiency",
        "fact_penalty",
        "fact_mds_quality",
        "agg_state_monthly_compliance",
    }
)


GOLD_FOREIGN_KEYS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "fact_deficiency": (
        ("facility_key", "dim_facility", "facility_key"),
        ("survey_date_key", "dim_date", "date_key"),
        ("state_key", "dim_geography", "state_key"),
        ("deficiency_type_key", "dim_deficiency", "deficiency_type_key"),
    ),
    "fact_penalty": (
        ("facility_key", "dim_facility", "facility_key"),
        ("penalty_date_key", "dim_date", "date_key"),
        ("state_key", "dim_geography", "state_key"),
    ),
    "fact_mds_quality": (
        ("facility_key", "dim_facility", "facility_key"),
        ("period_end_date_key", "dim_date", "date_key"),
        ("state_key", "dim_geography", "state_key"),
        ("measure_key", "dim_measure", "measure_key"),
    ),
    "agg_state_monthly_compliance": (
        ("state_key", "dim_geography", "state_key"),
        ("month_date_key", "dim_date", "date_key"),
    ),
}


GOLD_BUSINESS_COLUMNS: dict[str, tuple[str, ...]] = {
    name: tuple(
        field.name
        for field in schema.fields
        if field.name not in set(GOLD_PRIMARY_KEYS[name]) | {"business_hash", "load_timestamp"}
    )
    for name, schema in GOLD_SCHEMAS.items()
}


@dataclass(frozen=True)
class GoldMergeMetrics:
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0


@dataclass(frozen=True)
class GoldRefreshResult:
    rows_read: int
    rows_aggregated: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    target_metrics: tuple[dict[str, int | str], ...]
    table_counts: tuple[dict[str, int | str], ...]


class GoldValidationError(RuntimeError):
    """Raised when a staged or deployed Gold contract is invalid."""


def gold_table_name(catalog: str, schema_name: str, object_name: str) -> str:
    if object_name not in GOLD_SCHEMAS:
        raise ValueError(f"Unknown Gold table: {object_name}")
    return qualified_name(catalog, schema_name, object_name)


def _sha256_json_columns(columns: Sequence[str]) -> Any:
    from carewatch.silver import sha256_json_columns

    return sha256_json_columns(columns)


def _date_key(column: str) -> Any:
    from pyspark.sql import functions as F

    return F.date_format(F.col(column), "yyyyMMdd").cast("int")


def _with_business_hash(df: Any, table_name: str) -> Any:
    from pyspark.sql import functions as F

    return (
        df.withColumn(
            "business_hash",
            _sha256_json_columns(GOLD_BUSINESS_COLUMNS[table_name]),
        )
        .withColumn("load_timestamp", F.current_timestamp())
        .select(*[field.name for field in GOLD_SCHEMAS[table_name].fields])
    )


def add_mds_period_end(df: Any) -> Any:
    """Validate a YYYYQn-YYYYQn label and derive its ending quarter date."""

    from pyspark.sql import functions as F

    value = F.col("measure_period")
    start_year = F.regexp_extract(value, r"^([0-9]{4})Q", 1).cast("int")
    start_quarter = F.regexp_extract(value, r"^[0-9]{4}Q([1-4])-", 1).cast("int")
    end_year = F.regexp_extract(value, r"-([0-9]{4})Q", 1).cast("int")
    end_quarter = F.regexp_extract(value, r"Q([1-4])$", 1).cast("int")
    valid_format = value.rlike(MDS_PERIOD_PATTERN)
    ordered = (start_year * F.lit(4) + start_quarter) <= (
        end_year * F.lit(4) + end_quarter
    )
    valid = valid_format & ordered
    return (
        df.withColumn("_period_valid", valid)
        .withColumn(
            "period_end_date",
            F.when(
                valid,
                F.last_day(
                    F.make_date(end_year, end_quarter * F.lit(3), F.lit(1))
                ),
            ).otherwise(F.lit(None).cast("date")),
        )
    )


def build_dim_facility(
    facility: Any, deficiency: Any, penalty: Any, mds: Any
) -> Any:
    from pyspark.sql import functions as F

    ccn = "cms_certification_number_ccn"
    all_ccns = (
        facility.select(ccn)
        .unionByName(deficiency.select(ccn))
        .unionByName(penalty.select(ccn))
        .unionByName(mds.select(ccn))
        .distinct()
        .withColumn("facility_key", _sha256_json_columns((ccn,)))
    )
    provider = facility.select(
        ccn,
        "provider_name",
        "ownership_type",
        "chain_id",
        "chain_name",
        "citytown",
        F.col("state").alias("current_state_code"),
        "zip_code",
        "overall_rating",
        "reported_total_nurse_staffing_hours_per_resident_per_day",
        "total_nursing_staff_turnover",
        "number_of_certified_beds",
        F.col("source_processing_date").alias("profile_as_of_date"),
        F.col("facility_key").alias("silver_facility_key"),
        F.col("is_deleted").alias("provider_is_deleted"),
    )
    result = (
        all_ccns.join(provider, ccn, "left")
        .withColumn(
            "is_currently_listed",
            F.col("silver_facility_key").isNotNull()
            & ~F.coalesce(F.col("provider_is_deleted"), F.lit(True)),
        )
        .drop("silver_facility_key", "provider_is_deleted")
    )
    return _with_business_hash(result, "dim_facility")


def build_dim_date(spark: Any, deficiency: Any, penalty: Any, mds_with_period: Any) -> Any:
    from pyspark.sql import functions as F

    bounds = (
        deficiency.select(F.col("survey_date").alias("event_date"))
        .unionByName(penalty.select(F.col("penalty_date").alias("event_date")))
        .unionByName(mds_with_period.select(F.col("period_end_date").alias("event_date")))
        .agg(
            F.trunc(F.min("event_date"), "month").alias("min_date"),
            F.max("event_date").alias("max_date"),
        )
        .first()
    )
    if not bounds or (bounds["min_date"] is None and bounds["max_date"] is None):
        return spark.createDataFrame([], GOLD_SCHEMAS["dim_date"])
    if bounds["min_date"] is None or bounds["max_date"] is None:
        raise GoldValidationError("Gold date bounds are inconsistent")
    start = bounds["min_date"].isoformat()
    end = bounds["max_date"].isoformat()
    dates = spark.sql(
        f"SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) "
        "AS calendar_date"
    )
    result = dates.select(
        _date_key("calendar_date").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").cast("int").alias("year"),
        F.quarter("calendar_date").cast("int").alias("quarter"),
        F.month("calendar_date").cast("int").alias("month_number"),
        F.trunc("calendar_date", "month").alias("month_start_date"),
        F.date_format("calendar_date", "yyyy-MM").alias("year_month"),
    )
    return _with_business_hash(result, "dim_date")


def build_dim_geography(facility: Any, deficiency: Any, penalty: Any, mds: Any) -> Any:
    from pyspark.sql import functions as F

    states = (
        facility.select("state")
        .unionByName(deficiency.select("state"))
        .unionByName(penalty.select("state"))
        .unionByName(mds.select("state"))
        .filter(F.col("state").isNotNull())
        .select(F.trim("state").alias("state_code"))
        .filter(F.length("state_code") > 0)
        .distinct()
        .select(
            F.col("state_code").alias("state_key"),
            "state_code",
            F.col("state_code").alias("state_label"),
        )
    )
    unknown = facility.sparkSession.createDataFrame(
        [(UNKNOWN_STATE_KEY, None, "Unknown state")],
        "state_key string, state_code string, state_label string",
    )
    return _with_business_hash(states.unionByName(unknown), "dim_geography")


def build_dim_deficiency(deficiency: Any) -> Any:
    result = deficiency.select(
        "deficiency_prefix", "deficiency_tag_number", "deficiency_category"
    ).distinct()
    result = result.withColumn(
        "deficiency_type_key",
        _sha256_json_columns(
            ("deficiency_prefix", "deficiency_tag_number", "deficiency_category")
        ),
    )
    return _with_business_hash(result, "dim_deficiency")


def build_dim_measure(mds: Any) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    keys = ("measure_code", "resident_type")
    ordering = Window.partitionBy(*keys).orderBy(
        F.col("source_processing_date").desc_nulls_last(),
        F.col("load_timestamp").desc(),
    )
    ranked = mds.withColumn("_rank", F.dense_rank().over(ordering))
    conflicts = (
        ranked.filter(F.col("_rank") == 1)
        .groupBy(*keys)
        .agg(
            F.countDistinct(
                F.coalesce("measure_description", F.lit("__NULL__"))
            ).alias("descriptions")
        )
        .filter(F.col("descriptions") > 1)
        .count()
    )
    if conflicts:
        raise GoldValidationError(
            f"dim_measure has {conflicts} latest-timestamp description conflict(s)"
        )
    result = (
        ranked.withColumn("_row", F.row_number().over(ordering))
        .filter(F.col("_row") == 1)
        .select(*keys, "measure_description")
        .withColumn("measure_key", _sha256_json_columns(keys))
    )
    return _with_business_hash(result, "dim_measure")


def build_fact_deficiency(
    deficiency: Any,
    dim_facility: Any,
    dim_date: Any,
    dim_geography: Any,
    dim_deficiency: Any,
) -> Any:
    from pyspark.sql import functions as F

    source = (
        deficiency.alias("s")
        .join(
            dim_facility.select("cms_certification_number_ccn", "facility_key").alias("f"),
            "cms_certification_number_ccn",
            "left",
        )
        .withColumn("survey_date_key", _date_key("survey_date"))
        .withColumn(
            "_event_state_key",
            F.coalesce(F.trim(F.col("s.state")), F.lit(UNKNOWN_STATE_KEY)),
        )
        .join(dim_date.select("date_key").alias("d"), F.col("survey_date_key") == F.col("d.date_key"), "left")
        .join(
            dim_geography.select("state_key").alias("g"),
            F.col("_event_state_key") == F.col("g.state_key"),
            "left",
        )
        .join(
            dim_deficiency.select(
                "deficiency_type_key",
                "deficiency_prefix",
                "deficiency_tag_number",
                "deficiency_category",
            ).alias("t"),
            F.col("s.deficiency_prefix").eqNullSafe(F.col("t.deficiency_prefix"))
            & F.col("s.deficiency_tag_number").eqNullSafe(F.col("t.deficiency_tag_number"))
            & F.col("s.deficiency_category").eqNullSafe(F.col("t.deficiency_category")),
            "left",
        )
        .select(
            F.col("s.deficiency_key").alias("deficiency_key"),
            "facility_key",
            "survey_date_key",
            F.col("g.state_key").alias("state_key"),
            "deficiency_type_key",
            F.col("s.survey_date").alias("survey_date"),
            F.col("s.severity_group").alias("severity_group"),
            F.col("s.scope_severity_code").alias("scope_severity_code"),
            F.col("s.deficiency_corrected").alias("deficiency_corrected"),
            F.col("s.correction_date").alias("correction_date"),
            F.lit(1).cast("int").alias("citation_count"),
            F.when(F.col("s.severity_group").isin("G-I", "J-L"), 1)
            .otherwise(0)
            .cast("int")
            .alias("serious_citation_count"),
            F.col("s.is_deleted").alias("is_deleted"),
            F.col("s.source_batch_id").alias("source_batch_id"),
            F.col("s.source_processing_date").alias("source_processing_date"),
        )
    )
    return _with_business_hash(source, "fact_deficiency")


def build_fact_penalty(
    penalty: Any, dim_facility: Any, dim_date: Any, dim_geography: Any
) -> Any:
    from pyspark.sql import functions as F

    source = (
        penalty.alias("s")
        .join(
            dim_facility.select("cms_certification_number_ccn", "facility_key"),
            "cms_certification_number_ccn",
            "left",
        )
        .withColumn("penalty_date_key", _date_key("penalty_date"))
        .withColumn(
            "_event_state_key",
            F.coalesce(F.trim(F.col("s.state")), F.lit(UNKNOWN_STATE_KEY)),
        )
        .join(dim_date.select("date_key"), F.col("penalty_date_key") == F.col("date_key"), "left")
        .join(
            dim_geography.select("state_key").alias("g"),
            F.col("_event_state_key") == F.col("g.state_key"),
            "left",
        )
        .select(
            F.col("s.penalty_key").alias("penalty_key"),
            "facility_key",
            "penalty_date_key",
            F.col("g.state_key").alias("state_key"),
            F.col("s.penalty_date").alias("penalty_date"),
            F.col("s.penalty_type").alias("penalty_type"),
            F.col("s.fine_amount").alias("fine_amount"),
            F.col("s.payment_denial_length_in_days").alias("payment_denial_length_in_days"),
            F.lit(1).cast("int").alias("penalty_count"),
            F.when(F.lower(F.col("s.penalty_type")) == "fine", 1)
            .otherwise(0)
            .cast("int")
            .alias("fine_count"),
            F.when(F.lower(F.col("s.penalty_type")) == "payment denial", 1)
            .otherwise(0)
            .cast("int")
            .alias("payment_denial_count"),
            F.col("s.is_deleted").alias("is_deleted"),
            F.col("s.source_batch_id").alias("source_batch_id"),
            F.col("s.source_processing_date").alias("source_processing_date"),
        )
    )
    return _with_business_hash(source, "fact_penalty")


def build_fact_mds_quality(
    mds_with_period: Any,
    dim_facility: Any,
    dim_date: Any,
    dim_geography: Any,
    dim_measure: Any,
) -> Any:
    from pyspark.sql import functions as F

    source = (
        mds_with_period.alias("s")
        .join(
            dim_facility.select("cms_certification_number_ccn", "facility_key"),
            "cms_certification_number_ccn",
            "left",
        )
        .withColumn("period_end_date_key", _date_key("period_end_date"))
        .withColumn(
            "_event_state_key",
            F.coalesce(F.trim(F.col("s.state")), F.lit(UNKNOWN_STATE_KEY)),
        )
        .join(dim_date.select("date_key"), F.col("period_end_date_key") == F.col("date_key"), "left")
        .join(
            dim_geography.select("state_key").alias("g"),
            F.col("_event_state_key") == F.col("g.state_key"),
            "left",
        )
        .join(
            dim_measure.select(
                "measure_key", "measure_code", "resident_type"
            ).alias("m"),
            (F.col("s.measure_code") == F.col("m.measure_code"))
            & (F.col("s.resident_type") == F.col("m.resident_type")),
            "left",
        )
        .select(
            F.col("s.mds_key").alias("mds_key"),
            "facility_key",
            "period_end_date_key",
            F.col("g.state_key").alias("state_key"),
            "measure_key",
            F.col("s.measure_period").alias("measure_period"),
            F.col("s.period_end_date").alias("period_end_date"),
            F.col("s.q1_measure_score").alias("q1_measure_score"),
            F.col("s.footnote_for_q1_measure_score").alias("footnote_for_q1_measure_score"),
            F.col("s.q2_measure_score").alias("q2_measure_score"),
            F.col("s.footnote_for_q2_measure_score").alias("footnote_for_q2_measure_score"),
            F.col("s.q3_measure_score").alias("q3_measure_score"),
            F.col("s.footnote_for_q3_measure_score").alias("footnote_for_q3_measure_score"),
            F.col("s.q4_measure_score").alias("q4_measure_score"),
            F.col("s.footnote_for_q4_measure_score").alias("footnote_for_q4_measure_score"),
            F.col("s.four_quarter_average_score").alias("four_quarter_average_score"),
            F.col("s.footnote_for_four_quarter_average_score").alias(
                "footnote_for_four_quarter_average_score"
            ),
            F.col("s.is_deleted").alias("is_deleted"),
            F.col("s.source_batch_id").alias("source_batch_id"),
            F.col("s.source_processing_date").alias("source_processing_date"),
        )
    )
    return _with_business_hash(source, "fact_mds_quality")


def build_agg_state_monthly_compliance(deficiency: Any, penalty: Any) -> Any:
    from pyspark.sql import functions as F

    d = (
        deficiency.filter(~F.col("is_deleted"))
        .withColumn("month_start", F.trunc("survey_date", "month"))
        .groupBy("state_key", "month_start")
        .agg(
            F.sum("citation_count").cast("bigint").alias("citation_count"),
            F.sum("serious_citation_count")
            .cast("bigint")
            .alias("serious_citation_count"),
        )
    )
    p = (
        penalty.filter(~F.col("is_deleted"))
        .withColumn("month_start", F.trunc("penalty_date", "month"))
        .groupBy("state_key", "month_start")
        .agg(
            F.sum("penalty_count").cast("bigint").alias("penalty_count"),
            F.sum("fine_count").cast("bigint").alias("fine_count"),
            F.sum("payment_denial_count")
            .cast("bigint")
            .alias("payment_denial_count"),
            F.sum("fine_amount").cast("decimal(20,2)").alias("fine_amount_total"),
        )
    )
    result = (
        d.alias("d")
        .join(
            p.alias("p"),
            (F.col("d.state_key") == F.col("p.state_key"))
            & (F.col("d.month_start") == F.col("p.month_start")),
            "full",
        )
        .select(
            F.coalesce(F.col("d.state_key"), F.col("p.state_key")).alias("state_key"),
            F.date_format(
                F.coalesce(F.col("d.month_start"), F.col("p.month_start")),
                "yyyyMMdd",
            )
            .cast("int")
            .alias("month_date_key"),
            F.coalesce(F.col("d.citation_count"), F.lit(0)).cast("bigint").alias("citation_count"),
            F.coalesce(F.col("d.serious_citation_count"), F.lit(0))
            .cast("bigint")
            .alias("serious_citation_count"),
            F.coalesce(F.col("p.penalty_count"), F.lit(0)).cast("bigint").alias("penalty_count"),
            F.coalesce(F.col("p.fine_count"), F.lit(0)).cast("bigint").alias("fine_count"),
            F.coalesce(F.col("p.payment_denial_count"), F.lit(0))
            .cast("bigint")
            .alias("payment_denial_count"),
            F.coalesce(F.col("p.fine_amount_total"), F.lit(0))
            .cast("decimal(20,2)")
            .alias("fine_amount_total"),
        )
    )
    return _with_business_hash(result, "agg_state_monthly_compliance")


def stage_gold_outputs(spark: Any, catalog: str, schema_name: str) -> tuple[dict[str, Any], dict[str, int]]:
    """Read current Silver once and build all nine complete staged outputs."""

    silver_names = {
        "facility": qualified_name(catalog, schema_name, "silver_facility"),
        "deficiency": qualified_name(catalog, schema_name, "silver_deficiency"),
        "penalty": qualified_name(catalog, schema_name, "silver_penalty"),
        "mds": qualified_name(catalog, schema_name, "silver_mds_quality"),
    }
    inputs = {name: spark.table(table).cache() for name, table in silver_names.items()}
    counts = {name: int(df.count()) for name, df in inputs.items()}
    silver_keys = {
        "facility": "facility_key",
        "deficiency": "deficiency_key",
        "penalty": "penalty_key",
        "mds": "mds_key",
    }
    for name, key in silver_keys.items():
        frame = inputs[name]
        null_keys = int(
            frame.filter(
                frame[key].isNull()
                | frame["cms_certification_number_ccn"].isNull()
            ).count()
        )
        duplicates = _duplicate_count(frame, (key,))
        if null_keys or duplicates:
            raise GoldValidationError(
                f"silver_{name} failed its input contract: "
                f"null_key_or_ccn={null_keys}, duplicate_keys={duplicates}"
            )
    mds_period = add_mds_period_end(inputs["mds"]).cache()
    invalid_periods = int(mds_period.filter("NOT _period_valid").count())
    if invalid_periods:
        raise GoldValidationError(
            f"silver_mds_quality contains {invalid_periods} invalid/backwards measure period(s)"
        )

    outputs: dict[str, Any] = {}
    outputs["dim_facility"] = build_dim_facility(
        inputs["facility"], inputs["deficiency"], inputs["penalty"], inputs["mds"]
    ).cache()
    outputs["dim_date"] = build_dim_date(
        spark, inputs["deficiency"], inputs["penalty"], mds_period
    ).cache()
    outputs["dim_geography"] = build_dim_geography(
        inputs["facility"], inputs["deficiency"], inputs["penalty"], inputs["mds"]
    ).cache()
    outputs["dim_deficiency"] = build_dim_deficiency(inputs["deficiency"]).cache()
    outputs["dim_measure"] = build_dim_measure(inputs["mds"]).cache()
    outputs["fact_deficiency"] = build_fact_deficiency(
        inputs["deficiency"],
        outputs["dim_facility"],
        outputs["dim_date"],
        outputs["dim_geography"],
        outputs["dim_deficiency"],
    ).cache()
    outputs["fact_penalty"] = build_fact_penalty(
        inputs["penalty"],
        outputs["dim_facility"],
        outputs["dim_date"],
        outputs["dim_geography"],
    ).cache()
    outputs["fact_mds_quality"] = build_fact_mds_quality(
        mds_period,
        outputs["dim_facility"],
        outputs["dim_date"],
        outputs["dim_geography"],
        outputs["dim_measure"],
    ).cache()
    outputs["agg_state_monthly_compliance"] = build_agg_state_monthly_compliance(
        outputs["fact_deficiency"], outputs["fact_penalty"]
    ).cache()
    return outputs, counts


def _duplicate_count(df: Any, keys: Sequence[str]) -> int:
    from pyspark.sql import functions as F

    return int(df.groupBy(*keys).count().filter(F.col("count") > 1).count())


def validate_staged_outputs(outputs: Mapping[str, Any], silver_counts: Mapping[str, int]) -> None:
    from pyspark.sql import functions as F

    for name, df in outputs.items():
        expected_columns = [field.name for field in GOLD_SCHEMAS[name].fields]
        if df.columns != expected_columns:
            raise GoldValidationError(
                f"{name} columns differ from contract: expected {expected_columns}, got {df.columns}"
            )
        keys = GOLD_PRIMARY_KEYS[name]
        null_key = F.col(keys[0]).isNull()
        for key in keys[1:]:
            null_key = null_key | F.col(key).isNull()
        if df.filter(null_key).limit(1).count():
            raise GoldValidationError(f"{name} contains a null primary-key value")
        duplicates = _duplicate_count(df, keys)
        if duplicates:
            raise GoldValidationError(f"{name} contains {duplicates} duplicate primary key(s)")
        if df.filter(F.col("business_hash").isNull()).limit(1).count():
            raise GoldValidationError(f"{name} contains a null business_hash")

    expected_fact_counts = {
        "fact_deficiency": silver_counts["deficiency"],
        "fact_penalty": silver_counts["penalty"],
        "fact_mds_quality": silver_counts["mds"],
    }
    for name, expected in expected_fact_counts.items():
        actual = int(outputs[name].count())
        if actual != expected:
            raise GoldValidationError(
                f"{name} row count {actual} does not match Silver {expected}"
            )

    for fact_name, relationships in GOLD_FOREIGN_KEYS.items():
        fact = outputs[fact_name]
        for fact_column, dimension_name, dimension_column in relationships:
            dimension = outputs[dimension_name].select(dimension_column).distinct()
            missing = int(
                fact.select(fact_column)
                .join(
                    dimension,
                    fact[fact_column] == dimension[dimension_column],
                    "left_anti",
                )
                .count()
            )
            if missing:
                raise GoldValidationError(
                    f"{fact_name}.{fact_column} has {missing} unresolved {dimension_name} key(s)"
                )

    fact = outputs["fact_deficiency"].filter(~F.col("is_deleted"))
    penalty = outputs["fact_penalty"].filter(~F.col("is_deleted"))
    aggregate = outputs["agg_state_monthly_compliance"]
    comparisons = (
        ("citation_count", fact.agg(F.sum("citation_count")).first()[0], aggregate.agg(F.sum("citation_count")).first()[0]),
        ("serious_citation_count", fact.agg(F.sum("serious_citation_count")).first()[0], aggregate.agg(F.sum("serious_citation_count")).first()[0]),
        ("penalty_count", penalty.agg(F.sum("penalty_count")).first()[0], aggregate.agg(F.sum("penalty_count")).first()[0]),
        ("fine_count", penalty.agg(F.sum("fine_count")).first()[0], aggregate.agg(F.sum("fine_count")).first()[0]),
        ("payment_denial_count", penalty.agg(F.sum("payment_denial_count")).first()[0], aggregate.agg(F.sum("payment_denial_count")).first()[0]),
        ("fine_amount_total", penalty.agg(F.sum("fine_amount")).first()[0], aggregate.agg(F.sum("fine_amount_total")).first()[0]),
    )
    for metric, detail_value, aggregate_value in comparisons:
        if (detail_value or 0) != (aggregate_value or 0):
            raise GoldValidationError(
                f"Aggregate reconciliation failed for {metric}: detail={detail_value}, aggregate={aggregate_value}"
            )


def _latest_delta_metrics(delta_table: Any) -> Mapping[str, str]:
    row = delta_table.history(1).select("operationMetrics").first()
    return dict(row["operationMetrics"] or {}) if row else {}


def merge_gold_table(
    spark: Any,
    source: Any,
    table_name: str,
    key_columns: Sequence[str],
    *,
    delete_missing: bool = False,
) -> GoldMergeMetrics:
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table_name)
    columns = source.columns
    assignments = {column: f"source.`{column}`" for column in columns}
    condition = " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in key_columns
    )
    merge = (
        target.alias("target")
        .merge(source.alias("source"), condition)
        .whenMatchedUpdate(
            condition="target.business_hash <> source.business_hash",
            set=assignments,
        )
        .whenNotMatchedInsert(values=assignments)
    )
    if delete_missing:
        merge = merge.whenNotMatchedBySourceDelete()
    merge.execute()
    metrics = _latest_delta_metrics(target)
    return GoldMergeMetrics(
        rows_inserted=int(metrics.get("numTargetRowsInserted", 0)),
        rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
        rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
    )


def validate_deployed_gold(
    spark: Any,
    catalog: str,
    schema_name: str,
    staged_outputs: Mapping[str, Any] | None = None,
) -> tuple[dict[str, int | str], ...]:
    from pyspark.sql import functions as F

    counts: list[dict[str, int | str]] = []
    banned = {
        "telephone_number",
        "provider_address",
        "provider_address_hash",
        "location",
        "raw_record",
        "corrupt_record",
        "source_file",
        "source_file_sha256",
    }
    tables = {
        name: spark.table(gold_table_name(catalog, schema_name, name))
        for name in GOLD_SCHEMAS
    }
    for name, df in tables.items():
        exposed = sorted(banned.intersection(df.columns))
        if exposed:
            raise GoldValidationError(f"{name} exposes prohibited columns: {exposed}")
        duplicates = _duplicate_count(df, GOLD_PRIMARY_KEYS[name])
        null_loads = int(df.filter(F.col("load_timestamp").isNull()).count())
        if duplicates or null_loads:
            raise GoldValidationError(
                f"{name} failed deployed validation: duplicate_keys={duplicates}, null_load_timestamp={null_loads}"
            )
        counts.append({"table": name, "rows": int(df.count())})
    for fact_name, relationships in GOLD_FOREIGN_KEYS.items():
        for fact_column, dimension_name, dimension_column in relationships:
            missing = int(
                tables[fact_name]
                .select(fact_column)
                .join(
                    tables[dimension_name].select(dimension_column).distinct(),
                    tables[fact_name][fact_column]
                    == tables[dimension_name][dimension_column],
                    "left_anti",
                )
                .count()
            )
            if missing:
                raise GoldValidationError(
                    f"Deployed {fact_name}.{fact_column} has {missing} orphan key(s)"
                )
    if staged_outputs is not None:
        # These tables represent the complete current Silver state. Dimensions
        # such as dates may retain unused members, but current facilities and
        # detailed/aggregate facts must have exactly the staged key set.
        for name in sorted(GOLD_CURRENT_STATE_TABLES):
            # Spark Connect accepts a list of USING columns here; passing the
            # schema contract's tuple is treated as a join expression instead.
            keys = list(GOLD_PRIMARY_KEYS[name])
            staged_keys = staged_outputs[name].select(*keys)
            deployed_keys = tables[name].select(*keys)
            missing = staged_keys.join(deployed_keys, keys, "left_anti").limit(1).count()
            stale = deployed_keys.join(staged_keys, keys, "left_anti").limit(1).count()
            if missing or stale:
                raise GoldValidationError(
                    f"Deployed {name} key set differs from current Silver: "
                    f"missing={bool(missing)}, stale={bool(stale)}"
                )
    return tuple(counts)


def deploy_operational_views(spark: Any, catalog: str, schema_name: str) -> None:
    logs = qualified_name(catalog, schema_name, "pipeline_execution_logs")
    drift = qualified_name(catalog, schema_name, "schema_drift_log")
    bronze_q = qualified_name(catalog, schema_name, "bronze_quarantine")
    silver_q = qualified_name(catalog, schema_name, "silver_quarantine")
    run_view = qualified_name(catalog, schema_name, "vw_gold_pipeline_runs")
    error_view = qualified_name(catalog, schema_name, "vw_gold_errors")
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {run_view} AS
        SELECT pipeline_run_id, run_id, batch_id, pipeline_layer, dataset,
               load_type, start_time, end_time,
               timestampdiff(SECOND, start_time, end_time) AS duration_seconds,
               status, rows_read, rows_aggregated, rows_inserted, rows_updated,
               rows_deleted, rows_quarantined, load_timestamp
        FROM {logs}
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {error_view} AS
        WITH errors AS (
          SELECT CAST(load_timestamp AS DATE) AS error_date, dataset,
                 'Raw-to-Bronze' AS source_layer, drift_type AS rule
          FROM {drift}
          UNION ALL
          SELECT CAST(q.load_timestamp AS DATE), q.dataset,
                 'Raw-to-Bronze', rule
          FROM {bronze_q} q LATERAL VIEW explode(q.failed_rules) e AS rule
          UNION ALL
          SELECT CAST(q.load_timestamp AS DATE), q.dataset,
                 'Bronze-to-Silver', rule
          FROM {silver_q} q LATERAL VIEW explode(q.failed_rules) e AS rule
          UNION ALL
          SELECT CAST(load_timestamp AS DATE), dataset, pipeline_layer,
                 'EXECUTION_FAILURE' AS rule
          FROM {logs} WHERE status = 'FAILURE'
        )
        SELECT error_date, dataset, source_layer, rule,
               COUNT(*) AS error_count
        FROM errors
        GROUP BY error_date, dataset, source_layer, rule
        """
    )


def successful_silver_batches_for_pipeline_run(
    spark: Any, log_table: str, pipeline_run_id: str | None
) -> tuple[str, ...]:
    from pyspark.sql import functions as F

    if not pipeline_run_id:
        return ()
    rows = (
        spark.table(log_table)
        .filter(F.col("pipeline_run_id") == F.lit(pipeline_run_id))
        .filter(F.col("pipeline_layer") == F.lit("Bronze-to-Silver"))
        .filter(F.col("status").isin("SUCCESS", "QUARANTINED_PARTIAL"))
        .select("batch_id")
        .distinct()
        .orderBy("batch_id")
        .collect()
    )
    return tuple(str(row["batch_id"]) for row in rows)


def refresh_gold(
    spark: Any,
    catalog: str,
    schema_name: str,
    audit_record: MutableMapping[str, Any] | None = None,
) -> GoldRefreshResult:
    """Build, validate, merge, and reconcile the complete current Gold state."""

    outputs, silver_counts = stage_gold_outputs(spark, catalog, schema_name)
    validate_staged_outputs(outputs, silver_counts)
    rows_read = sum(silver_counts.values())
    rows_aggregated = int(
        outputs["fact_deficiency"].filter("is_deleted = false").count()
        + outputs["fact_penalty"].filter("is_deleted = false").count()
    )
    if audit_record is not None:
        audit_record["rows_read"] = rows_read
        audit_record["rows_aggregated"] = rows_aggregated

    inserted = updated = deleted = 0
    target_metrics: list[dict[str, int | str]] = []
    order = (
        "dim_facility",
        "dim_date",
        "dim_geography",
        "dim_deficiency",
        "dim_measure",
        "fact_deficiency",
        "fact_penalty",
        "fact_mds_quality",
        "agg_state_monthly_compliance",
    )
    for name in order:
        metrics = merge_gold_table(
            spark,
            outputs[name],
            gold_table_name(catalog, schema_name, name),
            GOLD_PRIMARY_KEYS[name],
            delete_missing=name in GOLD_CURRENT_STATE_TABLES,
        )
        inserted += metrics.rows_inserted
        updated += metrics.rows_updated
        deleted += metrics.rows_deleted
        if audit_record is not None:
            audit_record["rows_inserted"] = inserted
            audit_record["rows_updated"] = updated
            audit_record["rows_deleted"] = deleted
        target_metrics.append(
            {
                "table": name,
                "rows_inserted": metrics.rows_inserted,
                "rows_updated": metrics.rows_updated,
                "rows_deleted": metrics.rows_deleted,
            }
        )

    table_counts = validate_deployed_gold(spark, catalog, schema_name, outputs)
    deploy_operational_views(spark, catalog, schema_name)
    return GoldRefreshResult(
        rows_read=rows_read,
        rows_aggregated=rows_aggregated,
        rows_inserted=inserted,
        rows_updated=updated,
        rows_deleted=deleted,
        target_metrics=tuple(target_metrics),
        table_counts=table_counts,
    )
