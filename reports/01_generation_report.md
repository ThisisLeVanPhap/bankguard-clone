# 01 Data Generation Quality Report

## Project

- Project name: `digital_banking_fraud_monitoring`
- Data source strategy: synthetic source-system simulator
- Offline output: Parquet
- Streaming output: JSONL

## Row Counts

- `customers`: 10,000 rows
- `accounts`: 14,670 rows
- `cards`: 11,002 rows
- `merchants`: 3,000 rows
- `historical_transactions`: 367,200 rows
- `fraud_cases`: 4,236 rows
- `transaction_events`: 365,400 rows
- `login_events`: 80,000 rows
- `device_events`: 15,000 rows
- `fraud_alert_events`: 90,240 rows

## Core Quality Metrics

- Unique transaction count: 360,000
- Fraud rate on unique transactions: 1.1767%
- Duplicate transaction row rate: 1.9608%
- Duplicate stream event rate: 1.4778%
- Late-arriving transaction rate: 9.9161%
- Missing `device_id` rate: 69.6887%
- Missing `merchant_category` rate: 2.9888%

## Timestamp Checks

- Transactions where `created_ts >= event_timestamp`: 100.0000%
- Transaction event time range: 2025-01-01 00:00:18.947505830 to 2025-03-31 23:59:01.974113921
- Transaction created time range: 2025-01-01 00:01:57.538722755 to 2025-04-01 01:31:48.866534615

## Schema Evolution Check

- Schema change date: `2025-02-15`
- Old records before schema change: 184,040
- Old records missing `device_id`: 100.0000%
- Old records missing `ip_country`: 100.0000%
- Old records missing `channel`: 100.0000%

## Skew Checks

### Customer city distribution

- Ho Chi Minh City: 42.14%
- Ha Noi: 26.56%
- Da Nang: 8.03%
- Can Tho: 5.19%
- Other: 5.07%
- Binh Duong: 4.98%
- Dong Nai: 4.09%
- Hai Phong: 3.94%

### Merchant category distribution

- ecommerce: 20.47%
- grocery: 18.37%
- restaurant: 16.13%
- digital_goods: 9.70%
- travel: 8.90%
- cash_out: 8.03%
- electronics: 7.70%
- entertainment: 7.67%
- crypto: 3.03%

## Cardinality Checks

- `customers.customer_id` distinct count: 10,000
- `accounts.account_id` distinct count: 14,670
- `cards.card_id` distinct count: 11,002
- `merchants.merchant_id` distinct count: 3,000
- `historical_transactions.transaction_id` distinct count: 360,000
- `transaction_events.event_id` distinct count: 360,000

## Data Quality Challenges Injected

- Skewed city and merchant category distributions
- High-cardinality customer, transaction, device, merchant identifiers
- Duplicate transaction rows
- Duplicate streaming events
- Late-arriving events using `event_timestamp` vs `created_ts`
- Missing `device_id` and `merchant_category`
- Out-of-order streaming events
- Schema evolution before configured schema change date