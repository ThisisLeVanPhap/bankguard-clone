# 02 Data Quality Report

## Summary

- Total checks: 26
- Passed checks: 26
- Failed checks: 0

## Row Counts

| Table | Row Count |
|---|---:|
| `bronze.raw_historical_transactions` | 367,200 |
| `bronze.raw_transaction_events` | 365,400 |
| `silver.stg_transactions` | 360,000 |
| `silver.stg_transaction_events` | 360,000 |
| `gold.dim_customer` | 10,000 |
| `gold.dim_account` | 14,670 |
| `gold.dim_card` | 11,002 |
| `gold.dim_merchant` | 3,000 |
| `gold.dim_date` | 92 |
| `gold.fact_transaction` | 360,000 |
| `gold.fact_login_event` | 80,000 |
| `gold.fact_device_event` | 15,000 |
| `gold.fact_fraud_alert` | 90,240 |
| `gold.obt_transaction_risk` | 360,000 |
| `gold.obt_customer_risk_profile` | 10,000 |
| `gold.feat_customer_30d` | 10,000 |
| `gold.feat_transaction_realtime` | 360,000 |
| `gold.feat_customer_unified` | 360,000 |

## Check Results

| Check | Table | Column | Status | Details |
|---|---|---|---|---|
| dedup_effect | `stg_transactions` | `transaction_id` | **PASS** | bronze_rows=367,200, silver_rows=360,000, removed=7,200 |
| dedup_effect | `stg_transaction_events` | `event_id` | **PASS** | bronze_rows=365,400, silver_rows=360,000, removed=5,400 |
| unique_key | `dim_customer` | `customer_id` | **PASS** | rows=10,000, distinct=10,000, duplicate_count=0 |
| unique_key | `dim_account` | `account_id` | **PASS** | rows=14,670, distinct=14,670, duplicate_count=0 |
| unique_key | `dim_card` | `card_id` | **PASS** | rows=11,002, distinct=11,002, duplicate_count=0 |
| unique_key | `dim_merchant` | `merchant_id` | **PASS** | rows=3,000, distinct=3,000, duplicate_count=0 |
| unique_key | `fact_transaction` | `transaction_id` | **PASS** | rows=360,000, distinct=360,000, duplicate_count=0 |
| unique_key | `fact_login_event` | `event_id` | **PASS** | rows=80,000, distinct=80,000, duplicate_count=0 |
| unique_key | `fact_device_event` | `event_id` | **PASS** | rows=15,000, distinct=15,000, duplicate_count=0 |
| unique_key | `fact_fraud_alert` | `event_id` | **PASS** | rows=90,240, distinct=90,240, duplicate_count=0 |
| unique_key | `obt_transaction_risk` | `transaction_id` | **PASS** | rows=360,000, distinct=360,000, duplicate_count=0 |
| unique_key | `obt_customer_risk_profile` | `customer_id` | **PASS** | rows=10,000, distinct=10,000, duplicate_count=0 |
| unique_key | `feat_customer_30d` | `customer_id` | **PASS** | rows=10,000, distinct=10,000, duplicate_count=0 |
| unique_key | `feat_transaction_realtime` | `transaction_id` | **PASS** | rows=360,000, distinct=360,000, duplicate_count=0 |
| unique_key | `feat_customer_unified` | `transaction_id` | **PASS** | rows=360,000, distinct=360,000, duplicate_count=0 |
| not_null | `fact_transaction` | `transaction_id` | **PASS** | null_count=0 |
| not_null | `fact_transaction` | `customer_id` | **PASS** | null_count=0 |
| not_null | `fact_transaction` | `account_id` | **PASS** | null_count=0 |
| not_null | `fact_transaction` | `merchant_id` | **PASS** | null_count=0 |
| not_null | `obt_transaction_risk` | `transaction_id` | **PASS** | null_count=0 |
| not_null | `obt_transaction_risk` | `customer_id` | **PASS** | null_count=0 |
| non_negative | `fact_transaction` | `amount` | **PASS** | negative_count=0 |
| referential_integrity | `fact_transaction` | `customer_id` | **PASS** | missing_in_dim_customer=0 |
| referential_integrity | `fact_transaction` | `account_id` | **PASS** | missing_in_dim_account=0 |
| referential_integrity | `fact_transaction` | `merchant_id` | **PASS** | missing_in_dim_merchant=0 |
| date_coverage | `fact_transaction` | `date_key` | **PASS** | fact_dates_missing_in_dim_date=0 |