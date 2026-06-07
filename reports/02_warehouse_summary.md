# 02 Warehouse Summary

## Warehouse

- DuckDB database: `warehouse/finance.duckdb`
- Gold Parquet tables are published as DuckDB views.
- This DuckDB database acts as the local data warehouse / consumption layer for the mini-coursework.

## Published Tables

| Table | Row Count |
|---|---:|
| `dim_customer` | 10,000 |
| `dim_account` | 14,670 |
| `dim_card` | 11,002 |
| `dim_merchant` | 3,000 |
| `dim_date` | 92 |
| `dim_channel` | 6 |
| `fact_transaction` | 360,000 |
| `fact_login_event` | 80,000 |
| `fact_device_event` | 15,000 |
| `fact_fraud_alert` | 90,240 |
| `obt_transaction_risk` | 360,000 |
| `obt_customer_risk_profile` | 10,000 |
| `feat_customer_30d` | 10,000 |
| `feat_transaction_realtime` | 360,000 |
| `feat_customer_unified` | 360,000 |

## Sample Query: Fraud Rate by Channel

| channel   |   txn_count |   fraud_count |   fraud_rate |
|:----------|------------:|--------------:|-------------:|
| online    |       26917 |           392 |   0.0145633  |
| mobile    |       62824 |           855 |   0.0136094  |
| unknown   |      180475 |          2119 |   0.0117412  |
| web       |       25159 |           261 |   0.010374   |
| pos       |       46694 |           444 |   0.00950872 |
| atm       |       17931 |           165 |   0.00920194 |

## Sample Query: Fraud Rate by Merchant Category

| merchant_category   |   txn_count |   fraud_count |   fraud_rate |
|:--------------------|------------:|--------------:|-------------:|
| crypto              |       10993 |           185 |   0.0168289  |
| cash_out            |       28904 |           460 |   0.0159148  |
| digital_goods       |       34766 |           546 |   0.015705   |
| ecommerce           |       73629 |           930 |   0.0126309  |
| electronics         |       27558 |           343 |   0.0124465  |
| travel              |       31958 |           389 |   0.0121722  |
| entertainment       |       27573 |           256 |   0.00928444 |
| grocery             |       66194 |           605 |   0.0091398  |
| restaurant          |       58425 |           522 |   0.00893453 |

## Sample Query: High-Risk Customer Profiles

| customer_id   | customer_segment   | customer_city    | customer_risk_tier   |   txn_count_30d |   fraud_count_30d |   fraud_rate_30d |   declined_rate_30d |   max_alert_score_all |
|:--------------|:-------------------|:-----------------|:---------------------|----------------:|------------------:|-----------------:|--------------------:|----------------------:|
| C00004352     | student            | Binh Duong       | low                  |               2 |                 1 |         0.5      |            1        |                  0.6  |
| C00003434     | mass               | Ha Noi           | low                  |               5 |                 2 |         0.4      |            0.2      |                  0.7  |
| C00003344     | student            | Binh Duong       | low                  |               3 |                 1 |         0.333333 |            0.333333 |                  0.6  |
| C00007636     | student            | Ho Chi Minh City | low                  |               3 |                 1 |         0.333333 |            0        |                  0.5  |
| C00008488     | student            | Ho Chi Minh City | low                  |               3 |                 1 |         0.333333 |            0        |                  0.9  |
| C00009510     | student            | Ho Chi Minh City | low                  |               3 |                 1 |         0.333333 |            0        |                  0.45 |
| C00009682     | student            | Ha Noi           | medium               |               3 |                 1 |         0.333333 |            0        |                  0.4  |
| C00005392     | mass               | Dong Nai         | low                  |               7 |                 2 |         0.285714 |            0.142857 |                  0.9  |
| C00000306     | mass               | Ho Chi Minh City | low                  |              12 |                 3 |         0.25     |            0.166667 |                  0.6  |
| C00001568     | student            | Ho Chi Minh City | low                  |               4 |                 1 |         0.25     |            0        |                  0.4  |
