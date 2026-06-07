# Digital banking gold zone Schema Design

## 1. Goal

- Design and implement a Gold zone for digital banking fraud-risk monitoring.
- Support analytics, customer risk profiling, and feature construction.
- Use Fact-Dimension modeling with OBT and feature tables.

| Prefix | Meaning |
|---|---|
| raw_ | Bronze source-ingestion tables |
| stg_ | Silver cleaned staging tables |
| dim_ | Gold dimension tables |
| fact_ | Gold fact tables |
| obt_ | Gold denormalized serving tables |
| feat_ | Gold feature tables |

### 1.1 Storage Approach

| Layer | Storage |
|---|---|
| Bronze | Local Delta Lake tables under data/bronze/ |
| Silver | Local Delta Lake tables under data/silver/ |
| Gold | Parquet serving tables under data/gold/ |
| Warehouse | DuckDB database at warehouse/finance.duckdb |
| Governance | DataHub metadata, schema, ownership, tags, and lineage |

### 1.2 Input Data Profile

| Area | Details |
|---|---|
| Domain | Digital Banking / Fintech |
| Use case | Transaction fraud-risk monitoring |
| Historical range | 2025-01-01 to 2025-03-31 |
| Offline source format | Parquet |
| Streaming source format | JSONL |
| Main source transaction table | historical_transactions |
| Main streaming event table | transaction_events |
| Main timestamp columns | event_timestamp, created_ts |
| Main business keys | customer_id, account_id, card_id, merchant_id, transaction_id, event_id |

### 1.3 Source Datasets

| Source Dataset | Format | Purpose |
|---|---|---|
| customers | Parquet | Customer profile and segment data |
| accounts | Parquet | Bank account information |
| cards | Parquet | Card ownership and card status |
| merchants | Parquet | Merchant category, city, and risk level |
| historical_transactions | Parquet | Historical transaction records and fraud labels |
| fraud_cases | Parquet | Fraud case labels and review outcomes |
| transaction_events | JSONL | Streaming-style transaction events |
| login_events | JSONL | Login success/failure events |
| device_events | JSONL | Device registration/change/removal events |
| fraud_alert_events | JSONL | Rule-based fraud alert events |

### 1.4 Data Volume

| Dataset | Row Count |
|---|---:|
| bronze.raw_historical_transactions | 367,200 |
| bronze.raw_transaction_events | 365,400 |
| silver.stg_transactions | 360,000 |
| silver.stg_transaction_events | 360,000 |
| gold.dim_customer | 10,000 |
| gold.dim_account | 14,670 |
| gold.dim_card | 11,002 |
| gold.dim_merchant | 3,000 |
| gold.fact_transaction | 360,000 |
| gold.fact_login_event | 80,000 |
| gold.fact_device_event | 15,000 |
| gold.fact_fraud_alert | 90,240 |
| gold.obt_transaction_risk | 360,000 |
| gold.obt_customer_risk_profile | 10,000 |
| gold.feat_customer_30d | 10,000 |
| gold.feat_transaction_realtime | 360,000 |
| gold.feat_customer_unified | 360,000 |

### 1.5 Known Data Issues

The Section 01 generator intentionally injects:

- duplicate transaction rows;
- duplicate streaming events;
- late-arriving records using event_timestamp and created_ts;
- missing values in fields such as device_id and merchant_category;
- schema evolution before 2025-02-15;
- skewed city and merchant-category distributions;
- high-cardinality customer, transaction, device, and merchant identifiers.

### 1.6 Assumptions and SLA Targets

| Item | Local Coursework Implementation |
|---|---|
| Orchestration | Apache Airflow DAG |
| Bronze/Silver write mode | Idempotent overwrite to support reproducible reruns |
| Gold write mode | Rebuilt from Silver per run |
| Feature refresh | Rebuilt from Gold per run |
| Warehouse refresh | Recreated DuckDB views per run |
| DataHub | Metadata and lineage emitted after successful pipeline run |
| Airflow | Not implemented in the executed pipeline; planned as production extension |

| Layer | Target |
|---|---|
| Bronze ingestion freshness | <= 10 minutes after source availability |
| Silver transformation freshness | <= 30 minutes |
| Gold table freshness | <= 30 minutes |
| Feature table freshness | <= 60 minutes for batch features |
| Pipeline success target | >= 99% scheduled-run success in production |

---

## 2. Dimension Tables

| Dimension | Grain | Key Columns | Source | SCD Strategy |
|---|---|---|---|---|
| dim_customer | one row per customer | customer_key SK, customer_id BK | silver.stg_customers | SCD Type 1 |
| dim_account | one row per account | account_key SK, account_id BK | silver.stg_accounts | SCD Type 1 |
| dim_card | one row per card | card_key SK, card_id BK | silver.stg_cards | SCD Type 1 |
| dim_merchant | one row per merchant | merchant_key SK, merchant_id BK | silver.stg_merchants | SCD Type 1 |
| dim_date | one row per date | date_key | generated from transaction event range | Static |
| dim_channel | one row per channel | channel_key, channel | distinct channels from silver.stg_transactions | Static |

---

## 3. Fact Tables

Fact tables store transaction and event-level records with measures and risk flags.

### 3.1 fact_transaction

- Grain: one row per transaction_id.

- Source: silver.stg_transactions + Gold dimensions.

- Keys: transaction_id, customer_key, account_key, card_key, merchant_key, date_key, channel_key.

- Measures/flags: amount, is_fraud, is_declined, is_late_arrival, is_new_device, is_foreign_ip, is_high_risk_merchant.

### 3.2 fact_login_event

- Grain: one row per login event_id.

- Source: silver.stg_login_events.

- Measures and flags: login_status, failure_reason, is_login_success, is_login_failed, is_foreign_ip.

### 3.3 fact_device_event

- Grain: one row per device event_id.

- Source: silver.stg_device_events.

- Measures and flags: device_type, os, is_new_device, is_device_registered, is_device_changed, is_device_removed.

### 3.4 fact_fraud_alert

- Grain: one row per fraud alert event_id.

- Source: silver.stg_fraud_alert_events.

- Measures and flags: alert_rule, alert_score, alert_status, is_alert_open, is_alert_closed, is_alert_suppressed.

---

## 4. OBT Tables

### 4.1 obt_transaction_risk

Grain: one row per transaction.  
Purpose: transaction-level fraud-risk monitoring.  
Sources: fact_transaction, dimensions, and aggregated fact_fraud_alert.

### 4.2 obt_customer_risk_profile

Grain: one row per customer.  
Purpose: customer-level risk profile.  
Sources: transaction, login, device, fraud alert facts, and dim_customer.

---

## 5. Refresh and Data Quality

### 5.1 Refresh Strategy

| Layer | Local Implementation | Production Strategy |
|---|---|---|
| Bronze | overwrite Delta tables per run | append-only ingestion with batch_id, source offsets, and ingest metadata |
| Silver | overwrite cleaned Delta tables per run | incremental processing with deduplication by business key and event time |
| Gold | rebuild Parquet serving tables per run | incremental merge/upsert by stable keys |
| Feature tables | rebuild from Gold per run | rolling-window recomputation and merge by entity key and event timestamp |
| Warehouse | recreate DuckDB views per run | managed warehouse views or materialized tables |
| DataHub | emit metadata and lineage after pipeline run | scheduled metadata emission integrated with orchestrator |

### 5.2 Data Quality Checks

| Metric | Value |
|---|---:|
| Total checks | 26 |
| Passed checks | 26 |
| Failed checks | 0 |

Implemented checks:
- duplicate removal from Bronze to Silver.
- unique and not-null keys.
- referential integrity from facts to dimensions.
- date coverage and non-negative amount checks.

### 5.3 Deduplication Evidence

| Table | Bronze Rows | Silver Rows | Removed Rows |
|---|---:|---:|---:|
| historical_transactions / stg_transactions | 367,200 | 360,000 | 7,200 |
| transaction_events / stg_transaction_events | 365,400 | 360,000 | 5,400 |

---

## 6. Feature Store

| Feature Table | Grain | Purpose |
|---|---|---|
| feat_customer_30d | one row per customer | customer rolling 30-day transaction risk features |
| feat_transaction_realtime | one row per transaction | transaction-level real-time-ready risk flags |
| feat_customer_unified | one row per transaction | joined transaction and customer features |

Feature columns include rolling customer transaction metrics, transaction risk flags, alert signals, and unified customer-transaction features.

---

## 7. Data Pipeline Design and Implementation Scope

The implementation covers Bronze, Silver, Gold, feature pipelines, quality checks, warehouse publishing, and DataHub metadata emission.

### 7.1 Pipeline Groups

| Pipeline Group | Implementation | Output |
|---|---|---|
| Bronze ingestion | read source Parquet/JSONL and add ingest metadata | data/bronze/raw_* Delta tables |
| Silver transformation | clean, deduplicate, standardize timestamps, handle schema evolution | data/silver/stg_* Delta tables |
| Gold modeling | build dimensions, facts, and OBT tables | data/gold/dim_*, fact_*, obt_* |
| Feature pipeline | build customer and transaction feature tables | data/gold/feat_* |
| Warehouse publishing | publish Gold Parquet tables as DuckDB views | warehouse/finance.duckdb |
| Quality checks | validate row counts, uniqueness, nulls, referential integrity, date coverage | reports/02_data_quality_report.md |
| Metadata and lineage | emit dataset metadata, schema, tags, ownership, and lineage to DataHub | DataHub UI lineage graph |
| Orchestration | schedule and monitor Bronze → Silver → Gold → Feature → Quality → Warehouse → DataHub tasks | Airflow DAG |

### 7.2 Pipeline Update Strategy

- Bronze: append-only ingestion with ingest_ts, batch_id, and source offsets;
- Silver: incremental deduplication by business key and latest created_ts;
- Gold dimensions: SCD Type 1 or SCD Type 2 depending on attribute history requirements;
- Gold facts and OBTs: incremental merge/upsert by stable keys;
- Feature tables: rolling-window recomputation and merge by entity key plus event_timestamp;
- late-arriving data: reprocess affected windows and reconcile downstream Gold and feature tables.

### 7.3 Pipeline Controls and Monitoring

Implemented controls:

- run metadata with run_id, start/end timestamps, layer, status, input rows, output rows, and error summary.
- execution logs written to logs/02_pipeline_run.log.
- data quality report written to reports/02_data_quality_report.md.
- lineage summary written to reports/02_lineage_summary.md.
- warehouse summary written to reports/02_warehouse_summary.md.
- DataHub metadata and lineage emission through src/emit_datahub_metadata.py.

Current orchestration uses Apache Airflow. The DAG runs Bronze ingestion, Silver transformation, Gold modeling, feature generation, quality checks, warehouse publishing, and DataHub metadata emission in dependency order.

### 7.4 Lineage and Governance

DataHub publishes dataset metadata, schema, ownership, tags, and lineage for Bronze → Silver → Gold → Feature → Warehouse datasets.

---

## 8. Warehouse Optimization

### 8.1 Workload

Fraud-risk monitoring over transaction-level and customer-level Gold datasets.

### 8.2 Bottleneck

Repeated joins across fact, dimension, and alert tables.

### 8.3 Optimization Applied

Materialized obt_transaction_risk as a denormalized serving table and published Gold tables as DuckDB views.

### 8.4 Result

Warehouse evidence is stored in reports/02_warehouse_summary.md.

Published DuckDB views:

| Table | Row Count |
|---|---:|
| dim_customer | 10,000 |
| dim_account | 14,670 |
| dim_card | 11,002 |
| dim_merchant | 3,000 |
| dim_date | 92 |
| dim_channel | 6 |
| fact_transaction | 360,000 |
| fact_login_event | 80,000 |
| fact_device_event | 15,000 |
| fact_fraud_alert | 90,240 |
| obt_transaction_risk | 360,000 |
| obt_customer_risk_profile | 10,000 |
| feat_customer_30d | 10,000 |
| feat_transaction_realtime | 360,000 |
| feat_customer_unified | 360,000 |

### 8.5 Trade-off

The OBT table increases storage usage but improves query simplicity.

---

## 9. Deliverables

### 9.1 Code

| File | Purpose |
|---|---|
| src/generate_data.py | Source-system simulator |
| src/run_02_pipeline.py | Bronze, Silver, Gold, feature, and warehouse pipeline |
| src/quality_checks.py | Data quality validation |
| src/emit_datahub_metadata.py | DataHub metadata and lineage emitter |

### 9.2 Configuration

| File | Purpose |
|---|---|
| configs/generator_config.yaml | Data generator configuration |
| configs/pipeline_config.yaml | Pipeline paths, source system, and layer configuration |

### 9.3 Data Outputs

| Path | Purpose |
|---|---|
| data/source/offline/*.parquet | Offline source exports |
| data/source/stream/*.jsonl | Streaming-style source events |
| data/bronze/raw_* | Bronze Delta Lake tables |
| data/silver/stg_* | Silver Delta Lake tables |
| data/gold/*.parquet | Gold dimensions, facts, OBTs, and feature tables |
| warehouse/finance.duckdb | Local DuckDB warehouse |

### 9.4 Reports and Evidence

| File / Artifact | Purpose |
|---|---|
| reports/01_generation_report.md | Source generation quality report |
| reports/02_pipeline_run_metadata.csv | Pipeline run metadata |
| reports/02_lineage_summary.md | Local lineage summary |
| reports/02_data_quality_report.md | Data quality report |
| reports/02_warehouse_summary.md | DuckDB warehouse query evidence |
| logs/01_generate_data.log | Generator execution log |
| logs/02_pipeline_run.log | Pipeline execution log |
| DataHub UI screenshot | Governance and lineage evidence |
| Airflow DAG screenshot / task logs | Orchestration evidence |

### 9.5 Run Instructions

Run the Section 02 pipeline:

```bash
python src/run_02_pipeline.py --config configs/pipeline_config.yaml
```

Run quality checks:

```bash
python src/quality_checks.py --config configs/pipeline_config.yaml
```

Emit DataHub metadata and lineage:

```bash
python src/emit_datahub_metadata.py --config configs/pipeline_config.yaml --server http://localhost:8080
```

Run with Airflow:

```bash
airflow dags trigger digital_banking_fraud_monitoring_pipeline
```