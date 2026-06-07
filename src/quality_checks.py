from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from deltalake import DeltaTable


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_delta(path: Path) -> pd.DataFrame:
    return DeltaTable(str(path)).to_pandas()


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def check_unique(df: pd.DataFrame, table: str, key: str) -> dict[str, Any]:
    total = len(df)
    distinct = df[key].nunique(dropna=False)
    duplicate_count = total - distinct

    return {
        "check": "unique_key",
        "table": table,
        "column": key,
        "status": "PASS" if duplicate_count == 0 else "FAIL",
        "details": f"rows={total:,}, distinct={distinct:,}, duplicate_count={duplicate_count:,}",
    }


def check_not_null(df: pd.DataFrame, table: str, column: str) -> dict[str, Any]:
    null_count = int(df[column].isna().sum())

    return {
        "check": "not_null",
        "table": table,
        "column": column,
        "status": "PASS" if null_count == 0 else "FAIL",
        "details": f"null_count={null_count:,}",
    }


def check_non_negative(df: pd.DataFrame, table: str, column: str) -> dict[str, Any]:
    invalid_count = int((df[column] < 0).sum())

    return {
        "check": "non_negative",
        "table": table,
        "column": column,
        "status": "PASS" if invalid_count == 0 else "FAIL",
        "details": f"negative_count={invalid_count:,}",
    }


def check_referential(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    fact_table: str,
    dim_table: str,
    key: str,
) -> dict[str, Any]:
    fact_keys = set(fact_df[key].dropna().unique())
    dim_keys = set(dim_df[key].dropna().unique())
    missing = fact_keys - dim_keys

    return {
        "check": "referential_integrity",
        "table": fact_table,
        "column": key,
        "status": "PASS" if len(missing) == 0 else "FAIL",
        "details": f"missing_in_{dim_table}={len(missing):,}",
    }


def check_date_coverage(
    fact_transaction: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> dict[str, Any]:
    fact_dates = set(fact_transaction["date_key"].dropna().astype(int).unique())
    dim_dates = set(dim_date["date_key"].dropna().astype(int).unique())
    missing = fact_dates - dim_dates

    return {
        "check": "date_coverage",
        "table": "fact_transaction",
        "column": "date_key",
        "status": "PASS" if len(missing) == 0 else "FAIL",
        "details": f"fact_dates_missing_in_dim_date={len(missing):,}",
    }


def write_report(checks: list[dict[str, Any]], row_counts: dict[str, int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pass_count = sum(1 for c in checks if c["status"] == "PASS")
    fail_count = sum(1 for c in checks if c["status"] == "FAIL")

    lines = []
    lines.append("# 02 Data Quality Report")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total checks: {len(checks)}")
    lines.append(f"- Passed checks: {pass_count}")
    lines.append(f"- Failed checks: {fail_count}")
    lines.append("")

    lines.append("## Row Counts")
    lines.append("")
    lines.append("| Table | Row Count |")
    lines.append("|---|---:|")
    for table, count in row_counts.items():
        lines.append(f"| `{table}` | {count:,} |")
    lines.append("")

    lines.append("## Check Results")
    lines.append("")
    lines.append("| Check | Table | Column | Status | Details |")
    lines.append("|---|---|---|---|---|")

    for c in checks:
        lines.append(
            f"| {c['check']} | `{c['table']}` | `{c['column']}` | **{c['status']}** | {c['details']} |"
        )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    bronze_path = Path(config["paths"]["bronze_path"])
    silver_path = Path(config["paths"]["silver_path"])
    gold_path = Path(config["paths"]["gold_path"])
    output_path = Path(config["paths"]["quality_report_path"])

    # Bronze / Silver are Delta tables.
    raw_historical_transactions = read_delta(bronze_path / "raw_historical_transactions")
    raw_transaction_events = read_delta(bronze_path / "raw_transaction_events")

    stg_transactions = read_delta(silver_path / "stg_transactions")
    stg_transaction_events = read_delta(silver_path / "stg_transaction_events")

    # Gold tables are Parquet.
    dim_customer = read_parquet(gold_path / "dim_customer.parquet")
    dim_account = read_parquet(gold_path / "dim_account.parquet")
    dim_card = read_parquet(gold_path / "dim_card.parquet")
    dim_merchant = read_parquet(gold_path / "dim_merchant.parquet")
    dim_date = read_parquet(gold_path / "dim_date.parquet")

    fact_transaction = read_parquet(gold_path / "fact_transaction.parquet")
    fact_login_event = read_parquet(gold_path / "fact_login_event.parquet")
    fact_device_event = read_parquet(gold_path / "fact_device_event.parquet")
    fact_fraud_alert = read_parquet(gold_path / "fact_fraud_alert.parquet")

    obt_transaction_risk = read_parquet(gold_path / "obt_transaction_risk.parquet")
    obt_customer_risk_profile = read_parquet(gold_path / "obt_customer_risk_profile.parquet")

    feat_customer_30d = read_parquet(gold_path / "feat_customer_30d.parquet")
    feat_transaction_realtime = read_parquet(gold_path / "feat_transaction_realtime.parquet")
    feat_customer_unified = read_parquet(gold_path / "feat_customer_unified.parquet")

    row_counts = {
        "bronze.raw_historical_transactions": len(raw_historical_transactions),
        "bronze.raw_transaction_events": len(raw_transaction_events),
        "silver.stg_transactions": len(stg_transactions),
        "silver.stg_transaction_events": len(stg_transaction_events),
        "gold.dim_customer": len(dim_customer),
        "gold.dim_account": len(dim_account),
        "gold.dim_card": len(dim_card),
        "gold.dim_merchant": len(dim_merchant),
        "gold.dim_date": len(dim_date),
        "gold.fact_transaction": len(fact_transaction),
        "gold.fact_login_event": len(fact_login_event),
        "gold.fact_device_event": len(fact_device_event),
        "gold.fact_fraud_alert": len(fact_fraud_alert),
        "gold.obt_transaction_risk": len(obt_transaction_risk),
        "gold.obt_customer_risk_profile": len(obt_customer_risk_profile),
        "gold.feat_customer_30d": len(feat_customer_30d),
        "gold.feat_transaction_realtime": len(feat_transaction_realtime),
        "gold.feat_customer_unified": len(feat_customer_unified),
    }

    checks: list[dict[str, Any]] = []

    # Silver dedup evidence
    checks.append({
        "check": "dedup_effect",
        "table": "stg_transactions",
        "column": "transaction_id",
        "status": "PASS" if len(stg_transactions) == stg_transactions["transaction_id"].nunique() else "FAIL",
        "details": (
            f"bronze_rows={len(raw_historical_transactions):,}, "
            f"silver_rows={len(stg_transactions):,}, "
            f"removed={len(raw_historical_transactions) - len(stg_transactions):,}"
        ),
    })

    checks.append({
        "check": "dedup_effect",
        "table": "stg_transaction_events",
        "column": "event_id",
        "status": "PASS" if len(stg_transaction_events) == stg_transaction_events["event_id"].nunique() else "FAIL",
        "details": (
            f"bronze_rows={len(raw_transaction_events):,}, "
            f"silver_rows={len(stg_transaction_events):,}, "
            f"removed={len(raw_transaction_events) - len(stg_transaction_events):,}"
        ),
    })

    # Unique checks
    checks.extend([
        check_unique(dim_customer, "dim_customer", "customer_id"),
        check_unique(dim_account, "dim_account", "account_id"),
        check_unique(dim_card, "dim_card", "card_id"),
        check_unique(dim_merchant, "dim_merchant", "merchant_id"),
        check_unique(fact_transaction, "fact_transaction", "transaction_id"),
        check_unique(fact_login_event, "fact_login_event", "event_id"),
        check_unique(fact_device_event, "fact_device_event", "event_id"),
        check_unique(fact_fraud_alert, "fact_fraud_alert", "event_id"),
        check_unique(obt_transaction_risk, "obt_transaction_risk", "transaction_id"),
        check_unique(obt_customer_risk_profile, "obt_customer_risk_profile", "customer_id"),
        check_unique(feat_customer_30d, "feat_customer_30d", "customer_id"),
        check_unique(feat_transaction_realtime, "feat_transaction_realtime", "transaction_id"),
        check_unique(feat_customer_unified, "feat_customer_unified", "transaction_id"),
    ])

    # Not-null checks
    checks.extend([
        check_not_null(fact_transaction, "fact_transaction", "transaction_id"),
        check_not_null(fact_transaction, "fact_transaction", "customer_id"),
        check_not_null(fact_transaction, "fact_transaction", "account_id"),
        check_not_null(fact_transaction, "fact_transaction", "merchant_id"),
        check_not_null(obt_transaction_risk, "obt_transaction_risk", "transaction_id"),
        check_not_null(obt_transaction_risk, "obt_transaction_risk", "customer_id"),
    ])

    # Value checks
    checks.append(check_non_negative(fact_transaction, "fact_transaction", "amount"))

    # Referential checks
    checks.extend([
        check_referential(fact_transaction, dim_customer, "fact_transaction", "dim_customer", "customer_id"),
        check_referential(fact_transaction, dim_account, "fact_transaction", "dim_account", "account_id"),
        check_referential(fact_transaction, dim_merchant, "fact_transaction", "dim_merchant", "merchant_id"),
    ])

    # Date coverage
    checks.append(check_date_coverage(fact_transaction, dim_date))

    write_report(checks, row_counts, output_path)
    print(f"Wrote quality report to {output_path}")


if __name__ == "__main__":
    main()