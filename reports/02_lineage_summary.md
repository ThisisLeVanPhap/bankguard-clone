# 02 Lineage Summary

## Bronze Ingestion Lineage

| Source | Target | Pipeline |
|---|---|---|
| `data/source/offline/customers.parquet` | `data/bronze/raw_customers` | `bronze_ingest_offline_customers` |
| `data/source/offline/accounts.parquet` | `data/bronze/raw_accounts` | `bronze_ingest_offline_accounts` |
| `data/source/offline/cards.parquet` | `data/bronze/raw_cards` | `bronze_ingest_offline_cards` |
| `data/source/offline/merchants.parquet` | `data/bronze/raw_merchants` | `bronze_ingest_offline_merchants` |
| `data/source/offline/historical_transactions.parquet` | `data/bronze/raw_historical_transactions` | `bronze_ingest_offline_historical_transactions` |
| `data/source/offline/fraud_cases.parquet` | `data/bronze/raw_fraud_cases` | `bronze_ingest_offline_fraud_cases` |
| `data/source/stream/transaction_events.jsonl` | `data/bronze/raw_transaction_events` | `bronze_ingest_stream_transaction_events` |
| `data/source/stream/login_events.jsonl` | `data/bronze/raw_login_events` | `bronze_ingest_stream_login_events` |
| `data/source/stream/device_events.jsonl` | `data/bronze/raw_device_events` | `bronze_ingest_stream_device_events` |
| `data/source/stream/fraud_alert_events.jsonl` | `data/bronze/raw_fraud_alert_events` | `bronze_ingest_stream_fraud_alert_events` |
| `data/bronze/raw_customers` | `data/silver/stg_customers` | `silver_transform_customers` |
| `data/bronze/raw_accounts` | `data/silver/stg_accounts` | `silver_transform_accounts` |
| `data/bronze/raw_cards` | `data/silver/stg_cards` | `silver_transform_cards` |
| `data/bronze/raw_merchants` | `data/silver/stg_merchants` | `silver_transform_merchants` |
| `data/bronze/raw_historical_transactions` | `data/silver/stg_transactions` | `silver_transform_transactions` |
| `data/bronze/raw_fraud_cases` | `data/silver/stg_fraud_cases` | `silver_transform_fraud_cases` |
| `data/bronze/raw_transaction_events` | `data/silver/stg_transaction_events` | `silver_transform_transaction_events` |
| `data/bronze/raw_login_events` | `data/silver/stg_login_events` | `silver_transform_login_events` |
| `data/bronze/raw_device_events` | `data/silver/stg_device_events` | `silver_transform_device_events` |
| `data/bronze/raw_fraud_alert_events` | `data/silver/stg_fraud_alert_events` | `silver_transform_fraud_alert_events` |
| `data/silver` | `data/gold/dim_customer.parquet` | `gold_build_dim_customer` |
| `data/silver` | `data/gold/dim_account.parquet` | `gold_build_dim_account` |
| `data/silver` | `data/gold/dim_card.parquet` | `gold_build_dim_card` |
| `data/silver` | `data/gold/dim_merchant.parquet` | `gold_build_dim_merchant` |
| `data/silver` | `data/gold/dim_date.parquet` | `gold_build_dim_date` |
| `data/silver` | `data/gold/dim_channel.parquet` | `gold_build_dim_channel` |
| `data/silver + data/gold/dimensions` | `data/gold/fact_transaction.parquet` | `gold_build_fact_transaction` |
| `data/silver + data/gold/dimensions` | `data/gold/fact_login_event.parquet` | `gold_build_fact_login_event` |
| `data/silver + data/gold/dimensions` | `data/gold/fact_device_event.parquet` | `gold_build_fact_device_event` |
| `data/silver + data/gold/dimensions` | `data/gold/fact_fraud_alert.parquet` | `gold_build_fact_fraud_alert` |
| `data/gold/facts + data/gold/dimensions` | `data/gold/obt_transaction_risk.parquet` | `gold_build_obt_transaction_risk` |
| `data/gold/facts + data/gold/dimensions` | `data/gold/obt_customer_risk_profile.parquet` | `gold_build_obt_customer_risk_profile` |
| `data/gold/facts + data/gold/obt` | `data/gold/feat_customer_30d.parquet` | `gold_build_feat_customer_30d` |
| `data/gold/facts + data/gold/obt` | `data/gold/feat_transaction_realtime.parquet` | `gold_build_feat_transaction_realtime` |
| `data/gold/facts + data/gold/obt` | `data/gold/feat_customer_unified.parquet` | `gold_build_feat_customer_unified` |
| `data/gold` | `warehouse/finance.duckdb` | `publish_gold_to_duckdb` |

## Mermaid Graph

```mermaid
graph LR
  "data/source/offline/customers.parquet" -->|"bronze_ingest_offline_customers"| "data/bronze/raw_customers"
  "data/source/offline/accounts.parquet" -->|"bronze_ingest_offline_accounts"| "data/bronze/raw_accounts"
  "data/source/offline/cards.parquet" -->|"bronze_ingest_offline_cards"| "data/bronze/raw_cards"
  "data/source/offline/merchants.parquet" -->|"bronze_ingest_offline_merchants"| "data/bronze/raw_merchants"
  "data/source/offline/historical_transactions.parquet" -->|"bronze_ingest_offline_historical_transactions"| "data/bronze/raw_historical_transactions"
  "data/source/offline/fraud_cases.parquet" -->|"bronze_ingest_offline_fraud_cases"| "data/bronze/raw_fraud_cases"
  "data/source/stream/transaction_events.jsonl" -->|"bronze_ingest_stream_transaction_events"| "data/bronze/raw_transaction_events"
  "data/source/stream/login_events.jsonl" -->|"bronze_ingest_stream_login_events"| "data/bronze/raw_login_events"
  "data/source/stream/device_events.jsonl" -->|"bronze_ingest_stream_device_events"| "data/bronze/raw_device_events"
  "data/source/stream/fraud_alert_events.jsonl" -->|"bronze_ingest_stream_fraud_alert_events"| "data/bronze/raw_fraud_alert_events"
  "data/bronze/raw_customers" -->|"silver_transform_customers"| "data/silver/stg_customers"
  "data/bronze/raw_accounts" -->|"silver_transform_accounts"| "data/silver/stg_accounts"
  "data/bronze/raw_cards" -->|"silver_transform_cards"| "data/silver/stg_cards"
  "data/bronze/raw_merchants" -->|"silver_transform_merchants"| "data/silver/stg_merchants"
  "data/bronze/raw_historical_transactions" -->|"silver_transform_transactions"| "data/silver/stg_transactions"
  "data/bronze/raw_fraud_cases" -->|"silver_transform_fraud_cases"| "data/silver/stg_fraud_cases"
  "data/bronze/raw_transaction_events" -->|"silver_transform_transaction_events"| "data/silver/stg_transaction_events"
  "data/bronze/raw_login_events" -->|"silver_transform_login_events"| "data/silver/stg_login_events"
  "data/bronze/raw_device_events" -->|"silver_transform_device_events"| "data/silver/stg_device_events"
  "data/bronze/raw_fraud_alert_events" -->|"silver_transform_fraud_alert_events"| "data/silver/stg_fraud_alert_events"
  "data/silver" -->|"gold_build_dim_customer"| "data/gold/dim_customer.parquet"
  "data/silver" -->|"gold_build_dim_account"| "data/gold/dim_account.parquet"
  "data/silver" -->|"gold_build_dim_card"| "data/gold/dim_card.parquet"
  "data/silver" -->|"gold_build_dim_merchant"| "data/gold/dim_merchant.parquet"
  "data/silver" -->|"gold_build_dim_date"| "data/gold/dim_date.parquet"
  "data/silver" -->|"gold_build_dim_channel"| "data/gold/dim_channel.parquet"
  "data/silver + data/gold/dimensions" -->|"gold_build_fact_transaction"| "data/gold/fact_transaction.parquet"
  "data/silver + data/gold/dimensions" -->|"gold_build_fact_login_event"| "data/gold/fact_login_event.parquet"
  "data/silver + data/gold/dimensions" -->|"gold_build_fact_device_event"| "data/gold/fact_device_event.parquet"
  "data/silver + data/gold/dimensions" -->|"gold_build_fact_fraud_alert"| "data/gold/fact_fraud_alert.parquet"
  "data/gold/facts + data/gold/dimensions" -->|"gold_build_obt_transaction_risk"| "data/gold/obt_transaction_risk.parquet"
  "data/gold/facts + data/gold/dimensions" -->|"gold_build_obt_customer_risk_profile"| "data/gold/obt_customer_risk_profile.parquet"
  "data/gold/facts + data/gold/obt" -->|"gold_build_feat_customer_30d"| "data/gold/feat_customer_30d.parquet"
  "data/gold/facts + data/gold/obt" -->|"gold_build_feat_transaction_realtime"| "data/gold/feat_transaction_realtime.parquet"
  "data/gold/facts + data/gold/obt" -->|"gold_build_feat_customer_unified"| "data/gold/feat_customer_unified.parquet"
  "data/gold" -->|"publish_gold_to_duckdb"| "warehouse/finance.duckdb"
```