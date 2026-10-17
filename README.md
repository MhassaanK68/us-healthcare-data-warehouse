# CareWatch: U.S. Nursing Home Quality Warehouse

CareWatch is a data engineering and business intelligence project for analysing the quality and regulatory performance of Medicare- and Medicaid-certified nursing homes in the United States.

The project uses public datasets from the Centers for Medicare & Medicaid Services (CMS), processes them through a Bronze–Silver–Gold lakehouse architecture with PySpark and Delta Lake, and prepares business-ready data for Power BI.

CareWatch is an analytical decision-support system. It does not provide medical advice or replace official CMS records.

## Project goals

CareWatch is designed to help families, healthcare regulators, nursing-home operators, investors, and analysts answer questions such as:

- Which facilities have the most serious or repeated deficiencies?
- Which states, ownership types, or facility chains have weaker quality and compliance outcomes?
- How do staffing levels and staff turnover relate to quality ratings?
- Which facilities receive the largest fines or payment denials?
- How do deficiencies, penalties, and quality measures change over time?



## Data sources

The source is the [CMS Provider Data Catalog](https://data.cms.gov/provider-data/), accessed through its bulk CSV downloads and public APIs.

The project currently uses four datasets:

- **Provider Information** (`4pq5-n9py`): facility identity, location, ownership type, beds, ratings, staffing, turnover, and chain information.
- **Health Deficiencies** (`r5ix-sfxw`): inspection citations, deficiency categories, severity, survey dates, and correction status.
- **Penalties** (`g6vv-u9sr`): fines and payment-denial events.
- **MDS Quality Measures** (`djen-97ju`): quarterly and four-quarter resident quality measures.

The common facility join key is `cms_certification_number_ccn` (CCN).

## Architecture

```text
CMS API and bulk CSV files
            |
            v
Bronze: raw source-shaped records and ingestion metadata
            |
            v
Silver: typed, validated, privacy-treated, deduplicated records
            |
            v
Gold: facts, dimensions, scorecards, and BI aggregates
            |
            v
Power BI dashboards
```



### Bronze

Bronze preserves source values as strings and adds operational metadata such as the source file, batch ID, load type, ingestion date, and `load_timestamp`. Distinct batches append logically; an explicit rerun atomically replaces only the same deterministic batch so ingestion remains auditable and idempotent.

### Silver

Silver applies explicit data types, standardises names and categories, validates business keys, removes duplicates, and uses Delta `MERGE` for idempotent inserts and updates. Invalid records are routed to quarantine rather than silently discarded.

Sensitive contact fields are excluded from Silver. Street addresses are hashed with a salt supplied through Databricks Secrets.

<!-- BEGIN DATA MODELS -->

## Bronze and Silver data models

These column lists are generated from the explicit `StructType` contracts in `src/carewatch/schemas.py`. Run `python scripts/refresh_readme_models.py` after a deliberate schema change.

Bronze stores every CMS source field as a nullable `STRING`; its seven ingestion fields have the types shown below. Bronze retains source duplicates and therefore has **no enforced row primary key**. `_batch_id` identifies a replayable source batch, while the listed business-key columns identify candidate entities within a validated source snapshot. Silver has one deterministic SHA-256 primary key per table and applies Delta `MERGE` on it.

### `bronze_nh_health_deficiencies` (Bronze)

Candidate business key: `cms_certification_number_ccn`, `survey_date`, `survey_type`, `deficiency_prefix`, `deficiency_tag_number`, `inspection_cycle`. No enforced primary key.

| Column | Type | Nullable | Role |
|---|---|---|---|
| `cms_certification_number_ccn` | `STRING` | Yes | Business key candidate |
| `provider_name` | `STRING` | Yes |  |
| `provider_address` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `survey_date` | `STRING` | Yes | Business key candidate |
| `survey_footnote` | `STRING` | Yes |  |
| `survey_type` | `STRING` | Yes | Business key candidate |
| `deficiency_prefix` | `STRING` | Yes | Business key candidate |
| `deficiency_category` | `STRING` | Yes |  |
| `deficiency_tag_number` | `STRING` | Yes | Business key candidate |
| `deficiency_description` | `STRING` | Yes |  |
| `scope_severity_code` | `STRING` | Yes |  |
| `deficiency_corrected` | `STRING` | Yes |  |
| `correction_date` | `STRING` | Yes |  |
| `inspection_cycle` | `STRING` | Yes | Business key candidate |
| `standard_deficiency` | `STRING` | Yes |  |
| `complaint_deficiency` | `STRING` | Yes |  |
| `infection_control_inspection_deficiency` | `STRING` | Yes |  |
| `citation_under_idr` | `STRING` | Yes |  |
| `citation_under_iidr` | `STRING` | Yes |  |
| `location` | `STRING` | Yes |  |
| `processing_date` | `STRING` | Yes |  |
| `_dataset_id` | `STRING` | No |  |
| `_source_file` | `STRING` | No |  |
| `_source_file_sha256` | `STRING` | No |  |
| `_batch_id` | `STRING` | No | Batch lineage |
| `_load_type` | `STRING` | No |  |
| `_ingest_date` | `DATE` | No |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `bronze_nh_penalties` (Bronze)

Candidate business key: `cms_certification_number_ccn`, `penalty_date`, `penalty_type`, `fine_id`, `payment_denial_start_date`. No enforced primary key.

| Column | Type | Nullable | Role |
|---|---|---|---|
| `cms_certification_number_ccn` | `STRING` | Yes | Business key candidate |
| `provider_name` | `STRING` | Yes |  |
| `provider_address` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `penalty_date` | `STRING` | Yes | Business key candidate |
| `penalty_type` | `STRING` | Yes | Business key candidate |
| `fine_id` | `STRING` | Yes | Business key candidate |
| `fine_amount` | `STRING` | Yes |  |
| `payment_denial_start_date` | `STRING` | Yes | Business key candidate |
| `payment_denial_length_in_days` | `STRING` | Yes |  |
| `location` | `STRING` | Yes |  |
| `processing_date` | `STRING` | Yes |  |
| `_dataset_id` | `STRING` | No |  |
| `_source_file` | `STRING` | No |  |
| `_source_file_sha256` | `STRING` | No |  |
| `_batch_id` | `STRING` | No | Batch lineage |
| `_load_type` | `STRING` | No |  |
| `_ingest_date` | `DATE` | No |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `bronze_nh_provider_info` (Bronze)

Candidate business key: `cms_certification_number_ccn`. No enforced primary key.

| Column | Type | Nullable | Role |
|---|---|---|---|
| `cms_certification_number_ccn` | `STRING` | Yes | Business key candidate |
| `provider_name` | `STRING` | Yes |  |
| `provider_address` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `telephone_number` | `STRING` | Yes |  |
| `provider_ssa_county_code` | `STRING` | Yes |  |
| `countyparish` | `STRING` | Yes |  |
| `urban` | `STRING` | Yes |  |
| `ownership_type` | `STRING` | Yes |  |
| `number_of_certified_beds` | `STRING` | Yes |  |
| `average_number_of_residents_per_day` | `STRING` | Yes |  |
| `average_number_of_residents_per_day_footnote` | `STRING` | Yes |  |
| `provider_type` | `STRING` | Yes |  |
| `provider_resides_in_hospital` | `STRING` | Yes |  |
| `legal_business_name` | `STRING` | Yes |  |
| `date_first_approved_to_provide_medicare_and_medicaid_services` | `STRING` | Yes |  |
| `chain_name` | `STRING` | Yes |  |
| `chain_id` | `STRING` | Yes |  |
| `number_of_facilities_in_chain` | `STRING` | Yes |  |
| `chain_average_overall_5star_rating` | `STRING` | Yes |  |
| `chain_average_health_inspection_rating` | `STRING` | Yes |  |
| `chain_average_staffing_rating` | `STRING` | Yes |  |
| `chain_average_qm_rating` | `STRING` | Yes |  |
| `continuing_care_retirement_community` | `STRING` | Yes |  |
| `special_focus_status` | `STRING` | Yes |  |
| `abuse_icon` | `STRING` | Yes |  |
| `high_performing_icon` | `STRING` | Yes |  |
| `most_recent_health_inspection_more_than_2_years_ago` | `STRING` | Yes |  |
| `provider_changed_ownership_in_last_12_months` | `STRING` | Yes |  |
| `with_a_resident_and_family_council` | `STRING` | Yes |  |
| `automatic_sprinkler_systems_in_all_required_areas` | `STRING` | Yes |  |
| `overall_rating` | `STRING` | Yes |  |
| `overall_rating_footnote` | `STRING` | Yes |  |
| `health_inspection_rating` | `STRING` | Yes |  |
| `health_inspection_rating_footnote` | `STRING` | Yes |  |
| `qm_rating` | `STRING` | Yes |  |
| `qm_rating_footnote` | `STRING` | Yes |  |
| `longstay_qm_rating` | `STRING` | Yes |  |
| `longstay_qm_rating_footnote` | `STRING` | Yes |  |
| `shortstay_qm_rating` | `STRING` | Yes |  |
| `shortstay_qm_rating_footnote` | `STRING` | Yes |  |
| `staffing_rating` | `STRING` | Yes |  |
| `staffing_rating_footnote` | `STRING` | Yes |  |
| `reported_staffing_footnote` | `STRING` | Yes |  |
| `physical_therapist_staffing_footnote` | `STRING` | Yes |  |
| `reported_nurse_aide_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `reported_lpn_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `reported_rn_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `reported_licensed_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `reported_total_nurse_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14` | `STRING` | Yes |  |
| `registered_nurse_hours_per_resident_per_day_on_the_weekend` | `STRING` | Yes |  |
| `reported_physical_therapist_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `total_nursing_staff_turnover` | `STRING` | Yes |  |
| `total_nursing_staff_turnover_footnote` | `STRING` | Yes |  |
| `registered_nurse_turnover` | `STRING` | Yes |  |
| `registered_nurse_turnover_footnote` | `STRING` | Yes |  |
| `number_of_administrators_who_have_left_the_nursing_home` | `STRING` | Yes |  |
| `administrator_turnover_footnote` | `STRING` | Yes |  |
| `nursing_casemix_index` | `STRING` | Yes |  |
| `nursing_casemix_index_ratio` | `STRING` | Yes |  |
| `casemix_nurse_aide_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `casemix_lpn_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `casemix_rn_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `casemix_total_nurse_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `casemix_weekend_total_nurse_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `adjusted_nurse_aide_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `adjusted_lpn_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `adjusted_rn_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `adjusted_total_nurse_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day` | `STRING` | Yes |  |
| `rating_cycle_1_standard_survey_health_date` | `STRING` | Yes |  |
| `rating_cycle_1_standard_survey_footnote` | `STRING` | Yes |  |
| `rating_cycle_1_total_number_of_health_deficiencies` | `STRING` | Yes |  |
| `rating_cycle_1_number_of_standard_health_deficiencies` | `STRING` | Yes |  |
| `rating_cycle_1_number_of_complaint_health_deficiencies` | `STRING` | Yes |  |
| `rating_cycle_1_health_deficiency_score` | `STRING` | Yes |  |
| `rating_cycle_1_number_of_health_revisits` | `STRING` | Yes |  |
| `rating_cycle_1_health_revisit_score` | `STRING` | Yes |  |
| `rating_cycle_1_total_health_score` | `STRING` | Yes |  |
| `rating_cycle_2_standard_health_survey_date` | `STRING` | Yes |  |
| `rating_cycle_2_standard_survey_footnote` | `STRING` | Yes |  |
| `rating_cycle_23_total_number_of_health_deficiencies` | `STRING` | Yes |  |
| `rating_cycle_2_number_of_standard_health_deficiencies` | `STRING` | Yes |  |
| `rating_cycle_23_number_of_complaint_health_deficiencies` | `STRING` | Yes |  |
| `rating_cycle_23_health_deficiency_score` | `STRING` | Yes |  |
| `rating_cycle_23_number_of_health_revisits` | `STRING` | Yes |  |
| `rating_cycle_23_health_revisit_score` | `STRING` | Yes |  |
| `rating_cycle_23_total_health_score` | `STRING` | Yes |  |
| `total_weighted_health_survey_score` | `STRING` | Yes |  |
| `number_of_citations_from_infection_control_inspections` | `STRING` | Yes |  |
| `number_of_fines` | `STRING` | Yes |  |
| `total_amount_of_fines_in_dollars` | `STRING` | Yes |  |
| `number_of_payment_denials` | `STRING` | Yes |  |
| `total_number_of_penalties` | `STRING` | Yes |  |
| `location` | `STRING` | Yes |  |
| `latitude` | `STRING` | Yes |  |
| `longitude` | `STRING` | Yes |  |
| `geocoding_footnote` | `STRING` | Yes |  |
| `processing_date` | `STRING` | Yes |  |
| `_dataset_id` | `STRING` | No |  |
| `_source_file` | `STRING` | No |  |
| `_source_file_sha256` | `STRING` | No |  |
| `_batch_id` | `STRING` | No | Batch lineage |
| `_load_type` | `STRING` | No |  |
| `_ingest_date` | `DATE` | No |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `bronze_nh_mds_quality` (Bronze)

Candidate business key: `cms_certification_number_ccn`, `measure_code`, `resident_type`, `measure_period`. No enforced primary key.

| Column | Type | Nullable | Role |
|---|---|---|---|
| `cms_certification_number_ccn` | `STRING` | Yes | Business key candidate |
| `provider_name` | `STRING` | Yes |  |
| `provider_address` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `measure_code` | `STRING` | Yes | Business key candidate |
| `measure_description` | `STRING` | Yes |  |
| `resident_type` | `STRING` | Yes | Business key candidate |
| `q1_measure_score` | `STRING` | Yes |  |
| `footnote_for_q1_measure_score` | `STRING` | Yes |  |
| `q2_measure_score` | `STRING` | Yes |  |
| `footnote_for_q2_measure_score` | `STRING` | Yes |  |
| `q3_measure_score` | `STRING` | Yes |  |
| `footnote_for_q3_measure_score` | `STRING` | Yes |  |
| `q4_measure_score` | `STRING` | Yes |  |
| `footnote_for_q4_measure_score` | `STRING` | Yes |  |
| `four_quarter_average_score` | `STRING` | Yes |  |
| `footnote_for_four_quarter_average_score` | `STRING` | Yes |  |
| `used_in_quality_measure_five_star_rating` | `STRING` | Yes |  |
| `measure_period` | `STRING` | Yes | Business key candidate |
| `location` | `STRING` | Yes |  |
| `processing_date` | `STRING` | Yes |  |
| `_dataset_id` | `STRING` | No |  |
| `_source_file` | `STRING` | No |  |
| `_source_file_sha256` | `STRING` | No |  |
| `_batch_id` | `STRING` | No | Batch lineage |
| `_load_type` | `STRING` | No |  |
| `_ingest_date` | `DATE` | No |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `silver_deficiency` (Silver)

Primary key: `deficiency_key` (SHA-256 of the ordered business key: `cms_certification_number_ccn`, `survey_date`, `survey_type`, `deficiency_prefix`, `deficiency_tag_number`, `inspection_cycle`).

| Column | Type | Nullable | Key |
|---|---|---|---|
| `deficiency_key` | `STRING` | No | PK |
| `cms_certification_number_ccn` | `STRING` | No | BK1 |
| `provider_name` | `STRING` | Yes |  |
| `provider_address_hash` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `survey_date` | `DATE` | No | BK2 |
| `survey_footnote` | `INT` | Yes |  |
| `survey_type` | `STRING` | No | BK3 |
| `deficiency_prefix` | `STRING` | No | BK4 |
| `deficiency_category` | `STRING` | Yes |  |
| `deficiency_tag_number` | `STRING` | No | BK5 |
| `deficiency_description` | `STRING` | Yes |  |
| `scope_severity_code` | `STRING` | Yes |  |
| `severity_group` | `STRING` | Yes |  |
| `deficiency_corrected` | `STRING` | Yes |  |
| `correction_date` | `DATE` | Yes |  |
| `inspection_cycle` | `INT` | No | BK6 |
| `standard_deficiency` | `BOOLEAN` | Yes |  |
| `complaint_deficiency` | `BOOLEAN` | Yes |  |
| `infection_control_inspection_deficiency` | `BOOLEAN` | Yes |  |
| `citation_under_idr` | `BOOLEAN` | Yes |  |
| `citation_under_iidr` | `BOOLEAN` | Yes |  |
| `row_hash` | `STRING` | No |  |
| `is_deleted` | `BOOLEAN` | No |  |
| `source_file` | `STRING` | No |  |
| `source_file_sha256` | `STRING` | No |  |
| `source_batch_id` | `STRING` | No |  |
| `source_processing_date` | `DATE` | Yes |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `silver_penalty` (Silver)

Primary key: `penalty_key` (SHA-256 of the ordered business key: `cms_certification_number_ccn`, `penalty_date`, `penalty_type`, `fine_id`, `payment_denial_start_date`).

| Column | Type | Nullable | Key |
|---|---|---|---|
| `penalty_key` | `STRING` | No | PK |
| `cms_certification_number_ccn` | `STRING` | No | BK1 |
| `provider_name` | `STRING` | Yes |  |
| `provider_address_hash` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `penalty_date` | `DATE` | No | BK2 |
| `penalty_type` | `STRING` | No | BK3 |
| `fine_id` | `STRING` | Yes | BK4 |
| `fine_amount` | `DECIMAL(14,2)` | Yes |  |
| `payment_denial_start_date` | `DATE` | Yes | BK5 |
| `payment_denial_length_in_days` | `INT` | Yes |  |
| `row_hash` | `STRING` | No |  |
| `is_deleted` | `BOOLEAN` | No |  |
| `source_file` | `STRING` | No |  |
| `source_file_sha256` | `STRING` | No |  |
| `source_batch_id` | `STRING` | No |  |
| `source_processing_date` | `DATE` | Yes |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `silver_facility` (Silver)

Primary key: `facility_key` (SHA-256 of the ordered business key: `cms_certification_number_ccn`).

| Column | Type | Nullable | Key |
|---|---|---|---|
| `facility_key` | `STRING` | No | PK |
| `cms_certification_number_ccn` | `STRING` | No | BK1 |
| `provider_name` | `STRING` | Yes |  |
| `provider_address_hash` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `provider_ssa_county_code` | `STRING` | Yes |  |
| `countyparish` | `STRING` | Yes |  |
| `urban` | `BOOLEAN` | Yes |  |
| `ownership_type` | `STRING` | Yes |  |
| `provider_type` | `STRING` | Yes |  |
| `provider_resides_in_hospital` | `BOOLEAN` | Yes |  |
| `legal_business_name` | `STRING` | Yes |  |
| `date_first_approved_to_provide_medicare_and_medicaid_services` | `DATE` | Yes |  |
| `chain_name` | `STRING` | Yes |  |
| `chain_id` | `STRING` | Yes |  |
| `special_focus_status` | `STRING` | Yes |  |
| `with_a_resident_and_family_council` | `STRING` | Yes |  |
| `automatic_sprinkler_systems_in_all_required_areas` | `STRING` | Yes |  |
| `latitude` | `DOUBLE` | Yes |  |
| `longitude` | `DOUBLE` | Yes |  |
| `geocoding_footnote` | `INT` | Yes |  |
| `number_of_certified_beds` | `INT` | Yes |  |
| `average_number_of_residents_per_day` | `DOUBLE` | Yes |  |
| `average_number_of_residents_per_day_footnote` | `INT` | Yes |  |
| `number_of_facilities_in_chain` | `INT` | Yes |  |
| `chain_average_overall_5star_rating` | `DOUBLE` | Yes |  |
| `chain_average_health_inspection_rating` | `DOUBLE` | Yes |  |
| `chain_average_staffing_rating` | `DOUBLE` | Yes |  |
| `chain_average_qm_rating` | `DOUBLE` | Yes |  |
| `continuing_care_retirement_community` | `BOOLEAN` | Yes |  |
| `abuse_icon` | `BOOLEAN` | Yes |  |
| `high_performing_icon` | `BOOLEAN` | Yes |  |
| `most_recent_health_inspection_more_than_2_years_ago` | `BOOLEAN` | Yes |  |
| `provider_changed_ownership_in_last_12_months` | `BOOLEAN` | Yes |  |
| `overall_rating` | `INT` | Yes |  |
| `overall_rating_footnote` | `INT` | Yes |  |
| `health_inspection_rating` | `INT` | Yes |  |
| `health_inspection_rating_footnote` | `INT` | Yes |  |
| `qm_rating` | `INT` | Yes |  |
| `qm_rating_footnote` | `INT` | Yes |  |
| `longstay_qm_rating` | `INT` | Yes |  |
| `longstay_qm_rating_footnote` | `INT` | Yes |  |
| `shortstay_qm_rating` | `INT` | Yes |  |
| `shortstay_qm_rating_footnote` | `INT` | Yes |  |
| `staffing_rating` | `INT` | Yes |  |
| `staffing_rating_footnote` | `INT` | Yes |  |
| `reported_staffing_footnote` | `INT` | Yes |  |
| `physical_therapist_staffing_footnote` | `INT` | Yes |  |
| `reported_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `reported_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `reported_rn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `reported_licensed_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `reported_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14` | `DOUBLE` | Yes |  |
| `registered_nurse_hours_per_resident_per_day_on_the_weekend` | `DOUBLE` | Yes |  |
| `reported_physical_therapist_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `total_nursing_staff_turnover` | `DOUBLE` | Yes |  |
| `total_nursing_staff_turnover_footnote` | `INT` | Yes |  |
| `registered_nurse_turnover` | `DOUBLE` | Yes |  |
| `registered_nurse_turnover_footnote` | `INT` | Yes |  |
| `number_of_administrators_who_have_left_the_nursing_home` | `INT` | Yes |  |
| `administrator_turnover_footnote` | `INT` | Yes |  |
| `nursing_casemix_index` | `DOUBLE` | Yes |  |
| `nursing_casemix_index_ratio` | `DOUBLE` | Yes |  |
| `casemix_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `casemix_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `casemix_rn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `casemix_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `casemix_weekend_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `adjusted_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `adjusted_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `adjusted_rn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `adjusted_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  |
| `rating_cycle_1_standard_survey_health_date` | `DATE` | Yes |  |
| `rating_cycle_1_standard_survey_footnote` | `INT` | Yes |  |
| `rating_cycle_1_total_number_of_health_deficiencies` | `INT` | Yes |  |
| `rating_cycle_1_number_of_standard_health_deficiencies` | `INT` | Yes |  |
| `rating_cycle_1_number_of_complaint_health_deficiencies` | `INT` | Yes |  |
| `rating_cycle_1_health_deficiency_score` | `INT` | Yes |  |
| `rating_cycle_1_number_of_health_revisits` | `INT` | Yes |  |
| `rating_cycle_1_health_revisit_score` | `INT` | Yes |  |
| `rating_cycle_1_total_health_score` | `INT` | Yes |  |
| `rating_cycle_2_standard_health_survey_date` | `DATE` | Yes |  |
| `rating_cycle_2_standard_survey_footnote` | `INT` | Yes |  |
| `rating_cycle_23_total_number_of_health_deficiencies` | `INT` | Yes |  |
| `rating_cycle_2_number_of_standard_health_deficiencies` | `INT` | Yes |  |
| `rating_cycle_23_number_of_complaint_health_deficiencies` | `INT` | Yes |  |
| `rating_cycle_23_health_deficiency_score` | `INT` | Yes |  |
| `rating_cycle_23_number_of_health_revisits` | `INT` | Yes |  |
| `rating_cycle_23_health_revisit_score` | `INT` | Yes |  |
| `rating_cycle_23_total_health_score` | `INT` | Yes |  |
| `total_weighted_health_survey_score` | `DOUBLE` | Yes |  |
| `number_of_citations_from_infection_control_inspections` | `INT` | Yes |  |
| `number_of_fines` | `INT` | Yes |  |
| `total_amount_of_fines_in_dollars` | `DECIMAL(14,2)` | Yes |  |
| `number_of_payment_denials` | `INT` | Yes |  |
| `total_number_of_penalties` | `INT` | Yes |  |
| `row_hash` | `STRING` | No |  |
| `is_deleted` | `BOOLEAN` | No |  |
| `source_file` | `STRING` | No |  |
| `source_file_sha256` | `STRING` | No |  |
| `source_batch_id` | `STRING` | No |  |
| `source_processing_date` | `DATE` | Yes |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

### `silver_mds_quality` (Silver)

Primary key: `mds_key` (SHA-256 of the ordered business key: `cms_certification_number_ccn`, `measure_code`, `resident_type`, `measure_period`).

| Column | Type | Nullable | Key |
|---|---|---|---|
| `mds_key` | `STRING` | No | PK |
| `cms_certification_number_ccn` | `STRING` | No | BK1 |
| `provider_name` | `STRING` | Yes |  |
| `provider_address_hash` | `STRING` | Yes |  |
| `citytown` | `STRING` | Yes |  |
| `state` | `STRING` | Yes |  |
| `zip_code` | `STRING` | Yes |  |
| `measure_code` | `STRING` | No | BK2 |
| `measure_description` | `STRING` | Yes |  |
| `resident_type` | `STRING` | No | BK3 |
| `q1_measure_score` | `DOUBLE` | Yes |  |
| `footnote_for_q1_measure_score` | `INT` | Yes |  |
| `q2_measure_score` | `DOUBLE` | Yes |  |
| `footnote_for_q2_measure_score` | `INT` | Yes |  |
| `q3_measure_score` | `DOUBLE` | Yes |  |
| `footnote_for_q3_measure_score` | `INT` | Yes |  |
| `q4_measure_score` | `DOUBLE` | Yes |  |
| `footnote_for_q4_measure_score` | `INT` | Yes |  |
| `four_quarter_average_score` | `DOUBLE` | Yes |  |
| `footnote_for_four_quarter_average_score` | `INT` | Yes |  |
| `used_in_quality_measure_five_star_rating` | `BOOLEAN` | No |  |
| `measure_period` | `STRING` | No | BK4 |
| `row_hash` | `STRING` | No |  |
| `is_deleted` | `BOOLEAN` | No |  |
| `source_file` | `STRING` | No |  |
| `source_file_sha256` | `STRING` | No |  |
| `source_batch_id` | `STRING` | No |  |
| `source_processing_date` | `DATE` | Yes |  |
| `load_timestamp` | `TIMESTAMP` | No |  |

<!-- END DATA MODELS -->

## Gold layer (planned)

The planned Gold model contains:

- `dim_facility`
- `dim_geography`
- `dim_date`
- `dim_deficiency`
- `fact_deficiency`
- `fact_penalty`
- `fact_facility_snapshot`
- state-quality, ownership-comparison, facility-risk, and deficiency-trend aggregates

Power BI should connect to Gold tables or approved aggregate views, not directly to raw source files.

## Repository layout

```text
us-healthcare-warehouse/
├── data/
│   ├── raw/                         # Original downloads; ignored by Git
│   └── samples/
│       ├── full_load/               # Historical CSV parts
│       ├── incremental/             # Incremental API response samples
│       └── samples_manifest.json    # Source, row-count, and file metadata
├── docs/
│   ├── data_analysis_project_proposal.md
│   ├── phase2_guidelines.md
│   ├── bronze_silver_schema_contract.md
│   ├── phase2_evidence_walkthrough.md
│   └── evidence/phase2_live_outputs.ipynb
├── notebooks/
│   ├── 00_setup_tables.ipynb
│   ├── 01_acquire_cms.ipynb
│   ├── 02_raw_to_bronze.ipynb
│   ├── 03_bronze_to_silver.ipynb
│   ├── 04_migrate_silver_quarantine.ipynb
│   ├── 98_prepare_drift_demo.ipynb
│   ├── 99_demo_idempotency_drift_backfill.ipynb
│   ├── 99_phase2_submission_evidence.ipynb
│   └── 99_export_phase2_evidence.ipynb
├── src/carewatch/
├── scripts/refresh_readme_models.py
├── initial_ingestion_samples/
│   └── initial_fetch_for_samples.py
├── .gitignore
└── README.md
```

The runnable PySpark transformation and orchestration code is in `notebooks/` and `src/carewatch/`. The local contract tests validate schemas without a Databricks runtime.

## Preparing the sample data

The files under `data/samples` are local profiling and schema-development evidence only. **They are not the Phase 2 full-load input and must not be uploaded to Databricks.** The production Phase 2 flow starts with an empty landing area; Databricks discovers and streams the official CMS bulk files itself and creates incremental landing pages through the CMS API.

### Prerequisites

- Python 3.10 or later
- `requests`
- Internet access to `data.cms.gov`

Install the ingestion dependency:

```bash
python -m pip install requests
```

Download the CMS bulk files, split them into GitHub-safe parts, and create incremental samples:

```bash
python initial_ingestion_samples/initial_fetch_for_samples.py
```

The script defaults to:

- storing original downloads in `data/raw/`;
- storing version-controlled samples in `data/samples/`;
- keeping each CSV part below 45 MiB;
- requiring more than 210 MiB of historical sample data;
- using `2026-07-01` as the deficiency cutoff;
- using `2026-06-01` as the penalty cutoff;
- limiting each incremental API sample to 5,000 rows.

To rebuild the samples from files already present in `data/raw/` without downloading them again:

```bash
python initial_ingestion_samples/initial_fetch_for_samples.py --reuse
```

Useful overrides include:

```bash
python initial_ingestion_samples/initial_fetch_for_samples.py \
  --cutoff 2026-07-01 \
  --penalty-cutoff 2026-06-01 \
  --part-mb 45 \
  --min-mb 210 \
  --incr-rows 5000
```

Do not commit `data/raw/`. The generated manifest records the CMS dataset IDs, source URLs, catalog dates, split cutoffs, row counts, part sizes, and incremental filters.

## Phase 2 engineering requirements

The Bronze and Silver implementation must satisfy the following requirements:

1. Acquire initial/full loads directly inside Databricks from CMS-discovered bulk URLs; do not upload full-load files manually.
2. Acquire Health Deficiency and Penalty incrementals through paginated, date-windowed CMS API calls with a safety overlap and success-controlled watermarks.
3. Refresh Provider and MDS data through newly published CMS snapshots and merge only changed/new target rows.
4. Define every input with explicit PySpark `StructType` and `StructField` schemas; do not use `inferSchema`.
5. Add `load_timestamp` to every record in every table.
6. Run acquisition for all registered datasets automatically, using today UTC as the upper bound, a successful Bronze extraction watermark for normal API incrementals, and parameterized load type, paths, batch ID, catalog, schema, and landing root. Registry history starts are reserved for explicit backfills.
7. Support both standard incremental runs and reproducible historical backfills.
8. Preserve distinct source batches in Bronze with lineage; an explicit rerun replaces only the matching deterministic batch.
9. Use deterministic business keys, row hashes, and Delta `MERGE` in Silver.
10. Make reruns idempotent: processing the same input twice must not create duplicates.
11. Detect schema drift and either evolve compatible schemas deliberately or quarantine incompatible data.
12. Record acquisition manifests, watermarks, run status, timing, parameters, and row metrics.
13. Demonstrate automated full-load, incremental, idempotency, schema-drift, quarantine, and backfill scenarios.

See [Phase 2 guidelines](docs/phase2_guidelines.md) for the implementation sequence and evidence checklist.

### Running Raw-to-Bronze

Run `00_setup_tables.ipynb` and `01_acquire_cms.ipynb` before `02_raw_to_bronze.ipynb`. Raw-to-Bronze reads only successful records from `source_file_manifest`; it never scans the landing Volume for unregistered files.

The Raw-to-Bronze notebook accepts:

- `dataset`: one registry dataset or `all`;
- `load_type`: `full`, `incremental`, or `all`;
- `acquisition_run_id`: optional exact acquisition run;
- `source_path`: optional exact path that must already exist in the manifest;
- `reprocess`: defaults to `false`; set it to `true` only for deliberate batch replacement;
- `catalog` and `schema`: Unity Catalog namespace.

Each file or API page receives an independent execution-log row. Compatible added columns evolve the managed Delta Bronze table and are recorded in `schema_drift_log`. Missing required columns reject only that file. Corrupt CSV records enter `bronze_quarantine`, while valid records from the file continue to Bronze. A failed or partially quarantined acquisition run never advances its source watermark.

### Running Bronze-to-Silver

Before the first Step 7 run, `silver_quarantine` must have the current eight-column schema. If setup reports `LEGACY_SCHEMA_ACCEPTED_NO_MIGRATION`, run `04_migrate_silver_quarantine.ipynb` in its default `preview` mode. Preview is read-only. Preparing a candidate table and activating it are separate, approval-gated operations; activation retains the old table as `silver_quarantine_legacy_backup`.

`03_bronze_to_silver.ipynb` accepts:

- `dataset` and `load_type`, each defaulting to `all`;
- optional `acquisition_run_id` and comma-separated `batch_ids`;
- optional inclusive Bronze `ingest_from` and `ingest_to` dates for backfill;
- `reprocess`, which defaults to `false` and deliberately retries completed batches when enabled;
- `allow_soft_deletes`, which defaults to `false`;
- `address_secret_scope` and `address_secret_key`, which identify the Databricks Secret containing the stable address-hashing salt;
- `catalog` and `schema`.

With no scope filters, the notebook processes Raw-to-Bronze batches with accepted rows and no terminal Silver log, including partially quarantined Bronze batches. `SUCCESS` and `QUARANTINED_PARTIAL` are terminal Silver statuses; `FAILURE` remains retryable. It trims strings, converts empty values to null, safely casts values, applies the locked validation rules, removes clear-text contact fields, hashes addresses, generates deterministic keys and business hashes, collapses exact duplicates, quarantines invalid/conflicting rows, and performs an idempotent Delta `MERGE`.

The salt value is never returned to the Python driver or embedded with `F.lit`. The transformation invokes Databricks SQL `secret(scope, key)` inside Spark, so only the non-secret scope and key names appear in the submitted expression. Databricks secret redaction is best-effort, so the code never selects or logs the secret expression. A lookup or permission failure is captured by the per-batch audit boundary.

Batch reads and their materializing counts run inside that same audit boundary. An existing batch excluded by explicit ingestion dates is reported but receives no terminal Silver log. A date filter must include either all or none of a Bronze batch; a partial match fails instead of incorrectly checkpointing only part of a batch. A manifest/log batch with no Bronze rows is a failure. The notebook displays every failure after processing independent batches, then raises `RuntimeError` so a Databricks Job reports failure while already successful batches remain committed and are skipped on retry.

Identical matches are not updated, so their `load_timestamp` stays unchanged. Changed rows update only when the incoming `source_processing_date` is present and is not older than the current row; this prevents an old or undated backfill from replacing current SCD1 values. Backfill reads durable Bronze and never reacquires CMS data or advances `ingestion_watermarks`.

Soft deletion is disabled unless `allow_soft_deletes=true`. Even then, it is blocked for API windows, filtered/reprocessed scopes, empty or unreconciled snapshots, multi-artifact snapshots, snapshots older than the newest validated Bronze snapshot, snapshots without a processing-date scope bound, and any snapshot with quarantined or conflicting rows. Eligible deletion is limited to target rows whose processing date is not newer than the snapshot bound. The current acquisition design produces one bulk artifact for Provider and MDS snapshots; a future multi-artifact snapshot requires a deliberate run-level deletion design rather than weakening this guard.

The source values for some CMS descriptive domains, especially deficiency correction status, are not formally enumerated in the locked contract. Step 7 enforces the documented conditional correction-date rule but does not invent a closed domain that could reject a new valid CMS label.

## Execution guide: incremental load and backfill

Run these notebooks from a Databricks Git folder containing this repository. Use the same `catalog` and `schema` widgets for every step (defaults: `carewatch` and `pipeline`). Configure `address_secret_scope` and `address_secret_key` for `03_bronze_to_silver.ipynb`; they name a Databricks Secret containing a stable private address-hash salt. Do not enter the salt itself into a widget or Git.

**Initial full load:**

1. Run `notebooks/00_setup_tables.ipynb` once to create or verify the namespace, Volume, control tables, quarantine tables, and Silver tables.
2. Run `notebooks/01_acquire_cms.ipynb` with `load_type=full` (the default). It downloads all four current CMS bulk distributions into the landing Volume and registers them in `source_file_manifest`.
3. Run `notebooks/02_raw_to_bronze.ipynb` with `dataset=all`, `load_type=full`, and empty `acquisition_run_id` and `source_path`.
4. Run `notebooks/03_bronze_to_silver.ipynb` with `dataset=all`, `load_type=full`, empty scope filters, and the two secret-name widgets set. Leave `allow_soft_deletes=false`.

**Standard incremental load:** run `01_acquire_cms.ipynb` with `load_type=incremental`, then `02_raw_to_bronze.ipynb` with `dataset=all` and `load_type=incremental`, then `03_bronze_to_silver.ipynb` with `dataset=all` and `load_type=incremental` plus the secret-name widgets. Leave all backfill filters empty and `reprocess=false`. Health Deficiencies and Penalties use the last fully committed Bronze watermark minus `overlap_days` (default 60) through the current UTC date. Provider and MDS use newly published CMS snapshots. Complete the full load through Bronze first; the API incremental path intentionally fails if its Bronze watermark is missing.

**Backfill or rerun an already registered past batch:** find its `batch_id` and `acquisition_run_id` in `carewatch.pipeline.source_file_manifest` or `pipeline_execution_logs`. To replay Raw-to-Bronze, set `dataset=<dataset>`, `acquisition_run_id=<historical run ID>`, and `reprocess=true` in `02_raw_to_bronze.ipynb`; optionally set `source_path` to one exact manifest-listed file. Then run `03_bronze_to_silver.ipynb` with `dataset=<dataset>`, `batch_ids=<historical batch ID>` (comma-separated for several), `reprocess=true`, and the secret-name widgets. Leave `allow_soft_deletes=false`. For a Silver-only replay, skip the Raw step and run `03` directly on the existing Bronze batch. The alternative `ingest_from=YYYY-MM-DD` and `ingest_to=YYYY-MM-DD` widgets select a complete Bronze ingestion-date window; a partial batch selection is rejected. No code edits or new CMS download are needed for a registered batch. `01_acquire_cms.ipynb` does not currently expose an arbitrary historical CMS API date window, so this guide does not claim to acquire one.

After a run, inspect `carewatch.pipeline.pipeline_execution_logs` for `Raw-to-Bronze` and `Bronze-to-Silver` rows with the selected parameter, start/end times, status, and inserted/updated counts. Rerun the same Silver batch with `reprocess=true` and verify `rows_inserted=0`, `rows_updated=0`, unchanged Silver row count, and unchanged `load_timestamp` for matching keys. `notebooks/99_demo_idempotency_drift_backfill.ipynb` displays scoped manifest, count, drift, quarantine, and log views; set its `dataset` and optional `acquisition_run_id`/`batch_id` widgets to the run being demonstrated.

## Submission evidence

The rubric's saved-output proof is [phase2_live_outputs.ipynb](docs/evidence/phase2_live_outputs.ipynb). It contains actual Spark SQL results captured by [99_export_phase2_evidence.ipynb](notebooks/99_export_phase2_evidence.ipynb) in Databricks, with a final passing assertion. It covers physical schemas, `load_timestamp`, Silver key uniqueness, the first MERGE and no-op historical rerun, the isolated drift/quarantine run, and full/incremental execution logs. See the [evidence guide](docs/phase2_evidence_walkthrough.md) for job identifiers and reproduction steps.

## Data quality and security

- Telephone numbers must not be published to Silver or Gold.
- Street addresses should be hashed with SHA-256 and a salt supplied through a secret, never committed to Git.
- Invalid CCNs, dates, numeric values, and required keys must be quarantined with an actionable reason.
- CMS schema changes must not silently alter downstream tables.
- Raw source values and ingestion lineage must remain available for auditing.
- Missing numeric values must remain null rather than being converted to zero.



## Verification status

The four-dataset acquisition, Raw-to-Bronze, and Bronze-to-Silver paths are implemented in code. The saved Databricks outputs confirm populated Bronze/Silver tables, unique Silver entity keys, non-null `load_timestamp`, full and incremental logs, a no-op reprocess, and the controlled drift run in `phase2_demo` with one accepted and one quarantined Silver row. Gold tables and Power BI dashboards are outside the Phase 2 submission scope.



## Documentation

- [Project proposal](docs/data_analysis_project_proposal.md)
- [Phase 2 implementation guidelines](docs/phase2_guidelines.md)
- [Bronze and Silver schema contract](docs/bronze_silver_schema_contract.md)



## Team

- Hassan Mehmood — 24L2559, BDS-5B
- Hanan Ishaq — 24L2537, BDS-5B

