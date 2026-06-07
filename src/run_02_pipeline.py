from __future__ import annotations

import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import numpy as np
import pyarrow as pa
import uuid
import shutil
import yaml
import duckdb
from deltalake import DeltaTable, write_deltalake


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        filemode="w",
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    logging.getLogger().addHandler(console)


def ensure_dirs(config: dict[str, Any]) -> None:
    for key in ["bronze_path", "silver_path", "gold_path"]:
        Path(config["paths"][key]).mkdir(parents=True, exist_ok=True)

    Path(config["paths"]["report_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["quality_report_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["lineage_report_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["log_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["warehouse_path"]).parent.mkdir(parents=True, exist_ok=True)


def now_utc_str() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")


def read_jsonl(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def write_delta_table(df: pd.DataFrame, path: Path, mode: str = "overwrite") -> None:
    """Write a pandas DataFrame as a local Delta Lake table.

    Delta Lake stores Parquet data files plus a _delta_log transaction log.
    This is used for Bronze and Silver lakehouse tables in the mini-coursework.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    # Write to container-local temp storage first.
    # This avoids Delta Lake write failures on Windows bind-mounted folders.
    temp_path = Path("/tmp") / f"delta_write_{uuid.uuid4().hex}"

    try:
        write_deltalake(
            str(temp_path),
            arrow_table,
            mode="overwrite",
            schema_mode="overwrite",
        )
    except TypeError:
        write_deltalake(
            str(temp_path),
            arrow_table,
            mode="overwrite",
            overwrite_schema=True,
        )

    if mode == "overwrite" and path.exists():
        shutil.rmtree(path)

    shutil.copytree(temp_path, path)

    shutil.rmtree(temp_path, ignore_errors=True)
    
def read_delta_table(path: Path) -> pd.DataFrame:
    """Read a local Delta Lake table into pandas."""
    if not path.exists():
        raise FileNotFoundError(f"Delta table not found: {path}")
    return DeltaTable(str(path)).to_pandas()


def add_ingest_metadata(
    df: pd.DataFrame,
    batch_id: str,
    source_file: str,
    source_system: str,
) -> pd.DataFrame:
    df = df.copy()
    df["ingest_ts"] = now_utc_str()
    df["batch_id"] = batch_id
    df["source_file"] = source_file
    df["source_system"] = source_system
    return df


def ingest_offline_table(
    table_name: str,
    config: dict[str, Any],
    batch_id: str,
) -> dict[str, Any]:
    source_path = Path(config["paths"]["source_offline_path"]) / f"{table_name}.parquet"
    bronze_path = Path(config["paths"]["bronze_path"]) / f"raw_{table_name}"

    start_ts = now_utc_str()

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    df = pd.read_parquet(source_path)
    input_rows = len(df)

    df = add_ingest_metadata(
        df=df,
        batch_id=batch_id,
        source_file=str(source_path),
        source_system=config["source_system"]["name"],
    )

    write_delta_table(df, bronze_path, mode="overwrite")

    end_ts = now_utc_str()

    logging.info(
        "Bronze offline ingest success | table=%s | input=%s | output=%s",
        table_name,
        input_rows,
        len(df),
    )

    return {
        "run_id": batch_id,
        "pipeline_name": f"bronze_ingest_offline_{table_name}",
        "layer": "bronze",
        "source": str(source_path),
        "target": str(bronze_path),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "status": "SUCCESS",
        "input_rows": input_rows,
        "output_rows": len(df),
        "error_count": 0,
        "error_summary": "",
    }


def ingest_stream_table(
    table_name: str,
    config: dict[str, Any],
    batch_id: str,
) -> dict[str, Any]:
    source_path = Path(config["paths"]["source_stream_path"]) / f"{table_name}.jsonl"
    bronze_path = Path(config["paths"]["bronze_path"]) / f"raw_{table_name}"

    start_ts = now_utc_str()

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    df = read_jsonl(source_path)
    input_rows = len(df)

    df = add_ingest_metadata(
        df=df,
        batch_id=batch_id,
        source_file=str(source_path),
        source_system=config["source_system"]["name"],
    )

    write_delta_table(df, bronze_path, mode="overwrite")

    end_ts = now_utc_str()

    logging.info(
        "Bronze stream ingest success | table=%s | input=%s | output=%s",
        table_name,
        input_rows,
        len(df),
    )

    return {
        "run_id": batch_id,
        "pipeline_name": f"bronze_ingest_stream_{table_name}",
        "layer": "bronze",
        "source": str(source_path),
        "target": str(bronze_path),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "status": "SUCCESS",
        "input_rows": input_rows,
        "output_rows": len(df),
        "error_count": 0,
        "error_summary": "",
    }


def write_run_metadata(records: list[dict[str, Any]], config: dict[str, Any]) -> None:
    report_path = Path(config["paths"]["report_path"])
    report_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records)
    df.to_csv(report_path, index=False)

    logging.info("Wrote pipeline run metadata to %s", report_path)


def write_lineage_summary(records: list[dict[str, Any]], config: dict[str, Any]) -> None:
    path = Path(config["paths"]["lineage_report_path"])

    lines = []
    lines.append("# 02 Lineage Summary")
    lines.append("")
    lines.append("## Bronze Ingestion Lineage")
    lines.append("")
    lines.append("| Source | Target | Pipeline |")
    lines.append("|---|---|---|")

    for r in records:
        lines.append(f"| `{r['source']}` | `{r['target']}` | `{r['pipeline_name']}` |")

    lines.append("")
    lines.append("## Mermaid Graph")
    lines.append("")
    lines.append("```mermaid")
    lines.append("graph LR")

    for r in records:
        source_node = r["source"].replace("\\", "/")
        target_node = r["target"].replace("\\", "/")
        pipeline = r["pipeline_name"]
        lines.append(f'  "{source_node}" -->|"{pipeline}"| "{target_node}"')

    lines.append("```")

    path.write_text("\n".join(lines), encoding="utf-8")

    logging.info("Wrote lineage summary to %s", path)

def read_bronze_table(config: dict[str, Any], table_name: str) -> pd.DataFrame:
    path = Path(config["paths"]["bronze_path"]) / f"raw_{table_name}"
    return read_delta_table(path)


def write_silver_table(config: dict[str, Any], table_name: str, df: pd.DataFrame) -> Path:
    path = Path(config["paths"]["silver_path"]) / f"stg_{table_name}"
    write_delta_table(df, path, mode="overwrite")
    return path


def standardize_timestamps(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def dedup_latest(df: pd.DataFrame, key_cols: list[str], order_col: str = "created_ts") -> pd.DataFrame:
    df = df.copy()

    if order_col in df.columns:
        df[order_col] = pd.to_datetime(df[order_col], errors="coerce")
        df = df.sort_values(order_col)
    else:
        df = df.reset_index(drop=True)

    return df.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def add_silver_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = df.copy()
    df["silver_processed_ts"] = now_utc_str()
    df["silver_batch_id"] = batch_id
    return df

def transform_customers(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["signup_ts", "created_ts", "ingest_ts"])
    df = dedup_latest(df, ["customer_id"], "created_ts")
    df["city"] = df["city"].fillna("Unknown")
    df["province"] = df["province"].fillna("Unknown")
    df["customer_segment"] = df["customer_segment"].fillna("unknown")
    df["risk_tier"] = df["risk_tier"].fillna("unknown")
    return add_silver_metadata(df, batch_id)


def transform_accounts(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["open_ts", "close_ts", "created_ts", "ingest_ts"])
    df = dedup_latest(df, ["account_id"], "created_ts")
    df["account_type"] = df["account_type"].fillna("unknown")
    df["account_status"] = df["account_status"].fillna("unknown")
    return add_silver_metadata(df, batch_id)


def transform_cards(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["issued_ts", "created_ts", "ingest_ts"])
    df = dedup_latest(df, ["card_id"], "created_ts")
    df["card_type"] = df["card_type"].fillna("unknown")
    df["card_status"] = df["card_status"].fillna("unknown")
    return add_silver_metadata(df, batch_id)


def transform_merchants(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["created_ts", "ingest_ts"])
    df = dedup_latest(df, ["merchant_id"], "created_ts")
    df["merchant_category"] = df["merchant_category"].fillna("unknown")
    df["merchant_city"] = df["merchant_city"].fillna("Unknown")
    df["risk_level"] = df["risk_level"].fillna("unknown")
    return add_silver_metadata(df, batch_id)


def transform_transactions(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["event_timestamp", "created_ts", "ingest_ts"])

    before_rows = len(df)

    # Dedup duplicate transaction rows: keep the latest created_ts per transaction_id.
    df = dedup_latest(df, ["transaction_id"], "created_ts")

    # Normalize schema-evolution fields.
    for col in ["device_id", "ip_country", "channel", "merchant_category"]:
        if col in df.columns:
            df[col] = df[col].fillna("unknown")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"].notna()]
    df = df[df["amount"] >= 0]

    df["is_fraud"] = df["is_fraud"].fillna(False).astype(bool)
    df["is_late_arrival"] = df["is_late_arrival"].fillna(False).astype(bool)
    df["transaction_status"] = df["transaction_status"].fillna("unknown")
    df["transaction_type"] = df["transaction_type"].fillna("unknown")
    df["merchant_risk_level"] = df["merchant_risk_level"].fillna("unknown")

    df["silver_dedup_removed_rows"] = before_rows - len(df)

    return add_silver_metadata(df, batch_id)


def transform_fraud_cases(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["case_open_ts", "label_created_ts", "ingest_ts"])
    df = dedup_latest(df, ["case_id"], "label_created_ts")
    df["case_status"] = df["case_status"].fillna("unknown")
    df["fraud_type"] = df["fraud_type"].fillna("unknown")
    return add_silver_metadata(df, batch_id)


def transform_event_table(df: pd.DataFrame, batch_id: str, table_name: str) -> pd.DataFrame:
    df = standardize_timestamps(df, ["event_timestamp", "created_ts", "ingest_ts"])

    before_rows = len(df)

    if "event_id" in df.columns:
        df = dedup_latest(df, ["event_id"], "created_ts")

    # Common standardization
    if "event_type" in df.columns:
        df["event_type"] = df["event_type"].fillna("unknown")

    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].fillna("unknown")

    if "device_id" in df.columns:
        df["device_id"] = df["device_id"].fillna("unknown")

    if "ip_country" in df.columns:
        df["ip_country"] = df["ip_country"].fillna("unknown")

    if "alert_score" in df.columns:
        df["alert_score"] = pd.to_numeric(df["alert_score"], errors="coerce")
        df["alert_score"] = df["alert_score"].clip(lower=0, upper=1)

    df["silver_dedup_removed_rows"] = before_rows - len(df)
    df["silver_source_table"] = table_name

    return add_silver_metadata(df, batch_id)

def transform_to_silver_table(
    source_table: str,
    silver_table: str,
    transform_fn,
    config: dict[str, Any],
    batch_id: str,
) -> dict[str, Any]:
    start_ts = now_utc_str()

    bronze_path = Path(config["paths"]["bronze_path"]) / f"raw_{source_table}"
    silver_path = Path(config["paths"]["silver_path"]) / f"stg_{silver_table}"

    raw_df = read_bronze_table(config, source_table)
    input_rows = len(raw_df)

    silver_df = transform_fn(raw_df, batch_id)
    output_rows = len(silver_df)

    write_delta_table(silver_df, silver_path, mode="overwrite")

    end_ts = now_utc_str()

    logging.info(
        "Silver transform success | table=%s | input=%s | output=%s",
        silver_table,
        input_rows,
        output_rows,
    )

    return {
        "run_id": batch_id,
        "pipeline_name": f"silver_transform_{silver_table}",
        "layer": "silver",
        "source": str(bronze_path),
        "target": str(silver_path),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "status": "SUCCESS",
        "input_rows": input_rows,
        "output_rows": output_rows,
        "error_count": 0,
        "error_summary": "",
    }


def run_silver_transforms(config: dict[str, Any], batch_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    logging.info("Starting Silver transformations | batch_id=%s", batch_id)

    records.append(
        transform_to_silver_table(
            "customers",
            "customers",
            transform_customers,
            config,
            batch_id,
        )
    )

    records.append(
        transform_to_silver_table(
            "accounts",
            "accounts",
            transform_accounts,
            config,
            batch_id,
        )
    )

    records.append(
        transform_to_silver_table(
            "cards",
            "cards",
            transform_cards,
            config,
            batch_id,
        )
    )

    records.append(
        transform_to_silver_table(
            "merchants",
            "merchants",
            transform_merchants,
            config,
            batch_id,
        )
    )

    records.append(
        transform_to_silver_table(
            "historical_transactions",
            "transactions",
            transform_transactions,
            config,
            batch_id,
        )
    )

    records.append(
        transform_to_silver_table(
            "fraud_cases",
            "fraud_cases",
            transform_fraud_cases,
            config,
            batch_id,
        )
    )

    event_mappings = [
        ("transaction_events", "transaction_events"),
        ("login_events", "login_events"),
        ("device_events", "device_events"),
        ("fraud_alert_events", "fraud_alert_events"),
    ]

    for source_table, silver_table in event_mappings:
        records.append(
            transform_to_silver_table(
                source_table,
                silver_table,
                lambda df, b, source_table=source_table: transform_event_table(df, b, source_table),
                config,
                batch_id,
            )
        )

    logging.info("Silver transformations completed | batch_id=%s", batch_id)

    return records

def run_bronze_ingestion(config: dict[str, Any], batch_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    logging.info("Starting Bronze ingestion | batch_id=%s", batch_id)

    for table_name in config["bronze"]["offline_tables"]:
        records.append(ingest_offline_table(table_name, config, batch_id))

    for table_name in config["bronze"]["stream_tables"]:
        records.append(ingest_stream_table(table_name, config, batch_id))

    logging.info("Bronze ingestion completed | batch_id=%s", batch_id)

    return records

# Gold layer helpers

def read_silver_table(config: dict[str, Any], table_name: str) -> pd.DataFrame:
    path = Path(config["paths"]["silver_path"]) / f"stg_{table_name}"
    return read_delta_table(path)


def write_gold_table(config: dict[str, Any], table_name: str, df: pd.DataFrame) -> Path:
    path = Path(config["paths"]["gold_path"]) / f"{table_name}.parquet"
    write_parquet(df, path)
    return path


def add_surrogate_key(df: pd.DataFrame, business_key: str, surrogate_key: str) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(business_key).reset_index(drop=True)
    df.insert(0, surrogate_key, range(1, len(df) + 1))
    return df


def date_key_from_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y%m%d").astype("Int64")


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return np.where(denominator == 0, 0, numerator / denominator)


def add_gold_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    df = df.copy()
    df["gold_processed_ts"] = now_utc_str()
    df["gold_batch_id"] = batch_id
    return df

# Gold dimensions

def build_dim_customer(config: dict[str, Any], batch_id: str) -> pd.DataFrame:
    customers = read_silver_table(config, "customers")

    cols = [
        "customer_id",
        "full_name",
        "gender",
        "dob",
        "city",
        "province",
        "customer_segment",
        "risk_tier",
        "signup_ts",
        "created_ts",
    ]

    dim = customers[[c for c in cols if c in customers.columns]].copy()
    dim = dim.drop_duplicates("customer_id", keep="last")
    dim = add_surrogate_key(dim, "customer_id", "customer_key")

    return add_gold_metadata(dim, batch_id)


def build_dim_account(config: dict[str, Any], batch_id: str) -> pd.DataFrame:
    accounts = read_silver_table(config, "accounts")

    cols = [
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "open_ts",
        "close_ts",
        "created_ts",
    ]

    dim = accounts[[c for c in cols if c in accounts.columns]].copy()
    dim = dim.drop_duplicates("account_id", keep="last")
    dim = add_surrogate_key(dim, "account_id", "account_key")

    return add_gold_metadata(dim, batch_id)


def build_dim_card(config: dict[str, Any], batch_id: str) -> pd.DataFrame:
    cards = read_silver_table(config, "cards")

    cols = [
        "card_id",
        "customer_id",
        "account_id",
        "card_type",
        "card_status",
        "issued_ts",
        "created_ts",
    ]

    dim = cards[[c for c in cols if c in cards.columns]].copy()
    dim = dim.drop_duplicates("card_id", keep="last")
    dim = add_surrogate_key(dim, "card_id", "card_key")

    return add_gold_metadata(dim, batch_id)


def build_dim_merchant(config: dict[str, Any], batch_id: str) -> pd.DataFrame:
    merchants = read_silver_table(config, "merchants")

    cols = [
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "merchant_city",
        "risk_level",
        "created_ts",
    ]

    dim = merchants[[c for c in cols if c in merchants.columns]].copy()
    dim = dim.drop_duplicates("merchant_id", keep="last")
    dim = add_surrogate_key(dim, "merchant_id", "merchant_key")

    return add_gold_metadata(dim, batch_id)


def build_dim_date(config: dict[str, Any], batch_id: str) -> pd.DataFrame:
    txns = read_silver_table(config, "transactions")
    txns["event_timestamp"] = pd.to_datetime(txns["event_timestamp"], errors="coerce")

    min_date = txns["event_timestamp"].min().normalize() - pd.Timedelta(days=1)
    max_date = txns["event_timestamp"].max().normalize() + pd.Timedelta(days=1)

    dates = pd.date_range(min_date, max_date, freq="D")

    dim = pd.DataFrame({"calendar_date": dates})
    dim["date_key"] = dim["calendar_date"].dt.strftime("%Y%m%d").astype(int)
    dim["day_of_week"] = dim["calendar_date"].dt.day_name()
    dim["day_of_week_num"] = dim["calendar_date"].dt.dayofweek + 1
    dim["month"] = dim["calendar_date"].dt.month
    dim["quarter"] = dim["calendar_date"].dt.quarter
    dim["year"] = dim["calendar_date"].dt.year
    dim["is_weekend"] = dim["calendar_date"].dt.dayofweek.isin([5, 6])

    dim = dim[
        [
            "date_key",
            "calendar_date",
            "day_of_week",
            "day_of_week_num",
            "month",
            "quarter",
            "year",
            "is_weekend",
        ]
    ]

    return add_gold_metadata(dim, batch_id)


def build_dim_channel(config: dict[str, Any], batch_id: str) -> pd.DataFrame:
    txns = read_silver_table(config, "transactions")

    channels = sorted(txns["channel"].fillna("unknown").unique().tolist())

    dim = pd.DataFrame({"channel": channels})

    def channel_type(channel: str) -> str:
        if channel in ["mobile", "web", "online"]:
            return "digital"
        if channel in ["atm"]:
            return "self_service"
        if channel in ["pos"]:
            return "merchant_physical"
        return "unknown"

    dim["channel_type"] = dim["channel"].apply(channel_type)
    dim["is_digital_channel"] = dim["channel"].isin(["mobile", "web", "online"])
    dim = add_surrogate_key(dim, "channel", "channel_key")

    return add_gold_metadata(dim, batch_id)

# Gold facts

def build_fact_transaction(
    config: dict[str, Any],
    batch_id: str,
    dim_customer: pd.DataFrame,
    dim_account: pd.DataFrame,
    dim_card: pd.DataFrame,
    dim_merchant: pd.DataFrame,
    dim_channel: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    txns = read_silver_table(config, "transactions").copy()

    txns["event_timestamp"] = pd.to_datetime(txns["event_timestamp"], errors="coerce")
    txns["created_ts"] = pd.to_datetime(txns["created_ts"], errors="coerce")
    txns["date_key"] = date_key_from_ts(txns["event_timestamp"])
    txns["channel"] = txns["channel"].fillna("unknown")
    txns["card_id"] = txns["card_id"].fillna("unknown")
    txns["ip_country"] = txns["ip_country"].fillna("unknown")
    txns["merchant_risk_level"] = txns["merchant_risk_level"].fillna("unknown")

    fact = txns.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    fact = fact.merge(
        dim_account[["account_key", "account_id"]],
        on="account_id",
        how="left",
    )

    fact = fact.merge(
        dim_card[["card_key", "card_id"]],
        on="card_id",
        how="left",
    )

    fact = fact.merge(
        dim_merchant[["merchant_key", "merchant_id"]],
        on="merchant_id",
        how="left",
    )

    fact = fact.merge(
        dim_channel[["channel_key", "channel"]],
        on="channel",
        how="left",
    )

    fact = fact.merge(
        dim_date[["date_key", "is_weekend"]],
        on="date_key",
        how="left",
    )

    fact["is_declined"] = fact["transaction_status"].eq("declined")
    fact["is_authorized"] = fact["transaction_status"].eq("authorized")
    fact["is_settled"] = fact["transaction_status"].eq("settled")
    fact["is_foreign_ip"] = ~fact["ip_country"].isin(["VN", "unknown"])
    fact["is_high_risk_merchant"] = fact["merchant_risk_level"].eq("high")
    fact["is_night_transaction"] = fact["event_timestamp"].dt.hour.between(0, 5)

    cols = [
        "transaction_id",
        "customer_key",
        "account_key",
        "card_key",
        "merchant_key",
        "date_key",
        "channel_key",
        "customer_id",
        "account_id",
        "card_id",
        "merchant_id",
        "event_timestamp",
        "created_ts",
        "amount",
        "currency",
        "transaction_type",
        "transaction_status",
        "channel",
        "device_id",
        "ip_country",
        "schema_version",
        "is_fraud",
        "is_declined",
        "is_authorized",
        "is_settled",
        "is_late_arrival",
        "is_new_device",
        "is_foreign_ip",
        "is_high_risk_merchant",
        "is_night_transaction",
        "is_weekend",
    ]

    fact = fact[[c for c in cols if c in fact.columns]].copy()

    return add_gold_metadata(fact, batch_id)


def build_fact_login_event(
    config: dict[str, Any],
    batch_id: str,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    logins = read_silver_table(config, "login_events").copy()

    logins["event_timestamp"] = pd.to_datetime(logins["event_timestamp"], errors="coerce")
    logins["created_ts"] = pd.to_datetime(logins["created_ts"], errors="coerce")
    logins["date_key"] = date_key_from_ts(logins["event_timestamp"])
    logins["ip_country"] = logins["ip_country"].fillna("unknown")

    fact = logins.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    fact = fact.merge(
        dim_date[["date_key"]],
        on="date_key",
        how="left",
    )

    fact["is_login_success"] = fact["login_status"].eq("success")
    fact["is_login_failed"] = fact["login_status"].eq("failed")
    fact["is_foreign_ip"] = ~fact["ip_country"].isin(["VN", "unknown"])

    cols = [
        "event_id",
        "customer_key",
        "date_key",
        "customer_id",
        "event_timestamp",
        "created_ts",
        "device_id",
        "ip_country",
        "login_status",
        "failure_reason",
        "is_login_success",
        "is_login_failed",
        "is_foreign_ip",
    ]

    fact = fact[[c for c in cols if c in fact.columns]].copy()

    return add_gold_metadata(fact, batch_id)


def build_fact_device_event(
    config: dict[str, Any],
    batch_id: str,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    devices = read_silver_table(config, "device_events").copy()

    devices["event_timestamp"] = pd.to_datetime(devices["event_timestamp"], errors="coerce")
    devices["created_ts"] = pd.to_datetime(devices["created_ts"], errors="coerce")
    devices["date_key"] = date_key_from_ts(devices["event_timestamp"])

    fact = devices.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    fact = fact.merge(
        dim_date[["date_key"]],
        on="date_key",
        how="left",
    )

    fact["is_device_registered"] = fact["event_type"].eq("device_registered")
    fact["is_device_changed"] = fact["event_type"].eq("device_changed")
    fact["is_device_removed"] = fact["event_type"].eq("device_removed")

    cols = [
        "event_id",
        "customer_key",
        "date_key",
        "customer_id",
        "event_timestamp",
        "created_ts",
        "device_id",
        "device_type",
        "os",
        "event_type",
        "is_new_device",
        "is_device_registered",
        "is_device_changed",
        "is_device_removed",
    ]

    fact = fact[[c for c in cols if c in fact.columns]].copy()

    return add_gold_metadata(fact, batch_id)


def build_fact_fraud_alert(
    config: dict[str, Any],
    batch_id: str,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    alerts = read_silver_table(config, "fraud_alert_events").copy()

    alerts["event_timestamp"] = pd.to_datetime(alerts["event_timestamp"], errors="coerce")
    alerts["created_ts"] = pd.to_datetime(alerts["created_ts"], errors="coerce")
    alerts["date_key"] = date_key_from_ts(alerts["event_timestamp"])

    fact = alerts.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    fact = fact.merge(
        dim_date[["date_key"]],
        on="date_key",
        how="left",
    )

    fact["is_alert_open"] = fact["alert_status"].eq("open")
    fact["is_alert_closed"] = fact["alert_status"].eq("closed")
    fact["is_alert_suppressed"] = fact["alert_status"].eq("suppressed")

    cols = [
        "event_id",
        "transaction_id",
        "customer_key",
        "date_key",
        "customer_id",
        "event_timestamp",
        "created_ts",
        "alert_rule",
        "alert_score",
        "alert_status",
        "is_alert_open",
        "is_alert_closed",
        "is_alert_suppressed",
    ]

    fact = fact[[c for c in cols if c in fact.columns]].copy()

    return add_gold_metadata(fact, batch_id)

# Gold OBT / serving tables

def build_alert_aggregate(fact_fraud_alert: pd.DataFrame) -> pd.DataFrame:
    if len(fact_fraud_alert) == 0:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "alert_count",
                "max_alert_score",
                "first_alert_ts",
                "last_alert_ts",
                "alert_rules",
                "has_alert",
            ]
        )

    alerts = fact_fraud_alert.copy()
    alerts["event_timestamp"] = pd.to_datetime(alerts["event_timestamp"], errors="coerce")

    agg = alerts.groupby("transaction_id").agg(
        alert_count=("event_id", "count"),
        max_alert_score=("alert_score", "max"),
        first_alert_ts=("event_timestamp", "min"),
        last_alert_ts=("event_timestamp", "max"),
        alert_rules=("alert_rule", lambda x: ",".join(sorted(set(x.dropna().astype(str))))),
    ).reset_index()

    agg["has_alert"] = agg["alert_count"] > 0

    return agg


def build_obt_transaction_risk(
    batch_id: str,
    fact_transaction: pd.DataFrame,
    fact_fraud_alert: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_account: pd.DataFrame,
    dim_card: pd.DataFrame,
    dim_merchant: pd.DataFrame,
    dim_channel: pd.DataFrame,
) -> pd.DataFrame:
    obt = fact_transaction.copy()

    customer_cols = [
        "customer_key",
        "customer_id",
        "customer_segment",
        "risk_tier",
        "city",
        "province",
    ]
    obt = obt.merge(
        dim_customer[[c for c in customer_cols if c in dim_customer.columns]],
        on=["customer_key", "customer_id"],
        how="left",
        suffixes=("", "_customer"),
    )

    account_cols = [
        "account_key",
        "account_id",
        "account_type",
        "account_status",
    ]
    obt = obt.merge(
        dim_account[[c for c in account_cols if c in dim_account.columns]],
        on=["account_key", "account_id"],
        how="left",
    )

    card_cols = [
        "card_key",
        "card_id",
        "card_type",
        "card_status",
    ]
    obt = obt.merge(
        dim_card[[c for c in card_cols if c in dim_card.columns]],
        on=["card_key", "card_id"],
        how="left",
    )

    merchant_cols = [
        "merchant_key",
        "merchant_id",
        "merchant_category",
        "merchant_city",
        "risk_level",
    ]
    obt = obt.merge(
        dim_merchant[[c for c in merchant_cols if c in dim_merchant.columns]],
        on=["merchant_key", "merchant_id"],
        how="left",
    )

    channel_cols = [
        "channel_key",
        "channel",
        "channel_type",
        "is_digital_channel",
    ]
    obt = obt.merge(
        dim_channel[[c for c in channel_cols if c in dim_channel.columns]],
        on=["channel_key", "channel"],
        how="left",
    )

    alert_agg = build_alert_aggregate(fact_fraud_alert)
    obt = obt.merge(alert_agg, on="transaction_id", how="left")

    obt["alert_count"] = obt["alert_count"].fillna(0).astype(int)
    obt["max_alert_score"] = obt["max_alert_score"].fillna(0.0)
    obt["has_alert"] = obt["has_alert"].where(obt["has_alert"].notna(), False).astype(bool)
    obt["alert_rules"] = obt["alert_rules"].fillna("")

    obt = obt.rename(
        columns={
            "city": "customer_city",
            "province": "customer_province",
            "risk_tier": "customer_risk_tier",
            "risk_level": "merchant_risk_level_dim",
        }
    )

    return add_gold_metadata(obt, batch_id)


def build_obt_customer_risk_profile(
    batch_id: str,
    fact_transaction: pd.DataFrame,
    fact_login_event: pd.DataFrame,
    fact_device_event: pd.DataFrame,
    fact_fraud_alert: pd.DataFrame,
    dim_customer: pd.DataFrame,
) -> pd.DataFrame:
    txns = fact_transaction.copy()
    txns["event_timestamp"] = pd.to_datetime(txns["event_timestamp"], errors="coerce")

    max_ts = txns["event_timestamp"].max()
    window_30d_start = max_ts - pd.Timedelta(days=30)

    txns_30d = txns[txns["event_timestamp"] >= window_30d_start].copy()

    def txn_agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if len(df) == 0:
            return pd.DataFrame(columns=["customer_id"])

        agg = df.groupby("customer_id").agg(
            **{
                f"txn_count_{suffix}": ("transaction_id", "count"),
                f"txn_amount_sum_{suffix}": ("amount", "sum"),
                f"avg_txn_amount_{suffix}": ("amount", "mean"),
                f"max_txn_amount_{suffix}": ("amount", "max"),
                f"fraud_count_{suffix}": ("is_fraud", "sum"),
                f"declined_count_{suffix}": ("is_declined", "sum"),
                f"foreign_ip_txn_count_{suffix}": ("is_foreign_ip", "sum"),
                f"high_risk_merchant_txn_count_{suffix}": ("is_high_risk_merchant", "sum"),
                f"night_txn_count_{suffix}": ("is_night_transaction", "sum"),
                f"last_transaction_ts_{suffix}": ("event_timestamp", "max"),
            }
        ).reset_index()

        agg[f"fraud_rate_{suffix}"] = safe_divide(
            agg[f"fraud_count_{suffix}"],
            agg[f"txn_count_{suffix}"],
        )

        agg[f"declined_rate_{suffix}"] = safe_divide(
            agg[f"declined_count_{suffix}"],
            agg[f"txn_count_{suffix}"],
        )

        return agg

    profile = dim_customer[
        [
            "customer_id",
            "customer_segment",
            "city",
            "risk_tier",
        ]
    ].copy()

    all_agg = txn_agg(txns, "all")
    recent_agg = txn_agg(txns_30d, "30d")

    profile = profile.merge(all_agg, on="customer_id", how="left")
    profile = profile.merge(recent_agg, on="customer_id", how="left")

    # Login aggregates
    if len(fact_login_event) > 0:
        login_agg = fact_login_event.groupby("customer_id").agg(
            failed_login_count_all=("is_login_failed", "sum"),
            foreign_login_count_all=("is_foreign_ip", "sum"),
        ).reset_index()
        profile = profile.merge(login_agg, on="customer_id", how="left")

    # Device aggregates
    if len(fact_device_event) > 0:
        device_agg = fact_device_event.groupby("customer_id").agg(
            device_event_count_all=("event_id", "count"),
            new_device_event_count_all=("is_new_device", "sum"),
            device_change_count_all=("is_device_changed", "sum"),
        ).reset_index()
        profile = profile.merge(device_agg, on="customer_id", how="left")

    # Alert aggregates
    if len(fact_fraud_alert) > 0:
        alert_agg = fact_fraud_alert.groupby("customer_id").agg(
            fraud_alert_count_all=("event_id", "count"),
            max_alert_score_all=("alert_score", "max"),
        ).reset_index()
        profile = profile.merge(alert_agg, on="customer_id", how="left")

    numeric_cols = profile.select_dtypes(include=["number", "bool"]).columns
    profile[numeric_cols] = profile[numeric_cols].fillna(0)

    profile = profile.rename(
        columns={
            "city": "customer_city",
            "risk_tier": "customer_risk_tier",
        }
    )

    return add_gold_metadata(profile, batch_id)

# Gold runner

def run_gold_models(config: dict[str, Any], batch_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    logging.info("Starting Gold modeling | batch_id=%s", batch_id)

    start_ts = now_utc_str()

    dim_customer = build_dim_customer(config, batch_id)
    dim_account = build_dim_account(config, batch_id)
    dim_card = build_dim_card(config, batch_id)
    dim_merchant = build_dim_merchant(config, batch_id)
    dim_date = build_dim_date(config, batch_id)
    dim_channel = build_dim_channel(config, batch_id)

    dimensions = {
        "dim_customer": dim_customer,
        "dim_account": dim_account,
        "dim_card": dim_card,
        "dim_merchant": dim_merchant,
        "dim_date": dim_date,
        "dim_channel": dim_channel,
    }

    for table_name, df in dimensions.items():
        target = write_gold_table(config, table_name, df)
        records.append(
            {
                "run_id": batch_id,
                "pipeline_name": f"gold_build_{table_name}",
                "layer": "gold",
                "source": "data/silver",
                "target": str(target),
                "start_ts": start_ts,
                "end_ts": now_utc_str(),
                "status": "SUCCESS",
                "input_rows": "",
                "output_rows": len(df),
                "error_count": 0,
                "error_summary": "",
            }
        )
        logging.info("Gold dimension success | table=%s | output=%s", table_name, len(df))

    fact_transaction = build_fact_transaction(
        config,
        batch_id,
        dim_customer,
        dim_account,
        dim_card,
        dim_merchant,
        dim_channel,
        dim_date,
    )

    fact_login_event = build_fact_login_event(
        config,
        batch_id,
        dim_customer,
        dim_date,
    )

    fact_device_event = build_fact_device_event(
        config,
        batch_id,
        dim_customer,
        dim_date,
    )

    fact_fraud_alert = build_fact_fraud_alert(
        config,
        batch_id,
        dim_customer,
        dim_date,
    )

    facts = {
        "fact_transaction": fact_transaction,
        "fact_login_event": fact_login_event,
        "fact_device_event": fact_device_event,
        "fact_fraud_alert": fact_fraud_alert,
    }

    for table_name, df in facts.items():
        target = write_gold_table(config, table_name, df)
        records.append(
            {
                "run_id": batch_id,
                "pipeline_name": f"gold_build_{table_name}",
                "layer": "gold",
                "source": "data/silver + data/gold/dimensions",
                "target": str(target),
                "start_ts": start_ts,
                "end_ts": now_utc_str(),
                "status": "SUCCESS",
                "input_rows": "",
                "output_rows": len(df),
                "error_count": 0,
                "error_summary": "",
            }
        )
        logging.info("Gold fact success | table=%s | output=%s", table_name, len(df))

    obt_transaction_risk = build_obt_transaction_risk(
        batch_id,
        fact_transaction,
        fact_fraud_alert,
        dim_customer,
        dim_account,
        dim_card,
        dim_merchant,
        dim_channel,
    )

    obt_customer_risk_profile = build_obt_customer_risk_profile(
        batch_id,
        fact_transaction,
        fact_login_event,
        fact_device_event,
        fact_fraud_alert,
        dim_customer,
    )

    obts = {
        "obt_transaction_risk": obt_transaction_risk,
        "obt_customer_risk_profile": obt_customer_risk_profile,
    }

    for table_name, df in obts.items():
        target = write_gold_table(config, table_name, df)
        records.append(
            {
                "run_id": batch_id,
                "pipeline_name": f"gold_build_{table_name}",
                "layer": "gold",
                "source": "data/gold/facts + data/gold/dimensions",
                "target": str(target),
                "start_ts": start_ts,
                "end_ts": now_utc_str(),
                "status": "SUCCESS",
                "input_rows": "",
                "output_rows": len(df),
                "error_count": 0,
                "error_summary": "",
            }
        )
        logging.info("Gold OBT success | table=%s | output=%s", table_name, len(df))

    feature_records, _features = run_feature_pipelines(
        config=config,
        batch_id=batch_id,
        fact_transaction=fact_transaction,
        dim_customer=dim_customer,
        obt_transaction_risk=obt_transaction_risk,
    )
    records.extend(feature_records)

    logging.info("Gold modeling completed | batch_id=%s", batch_id)

    return records


# Gold feature tables

def build_feat_customer_30d(
    batch_id: str,
    fact_transaction: pd.DataFrame,
    dim_customer: pd.DataFrame,
) -> pd.DataFrame:
    txns = fact_transaction.copy()
    txns["event_timestamp"] = pd.to_datetime(txns["event_timestamp"], errors="coerce")

    max_ts = txns["event_timestamp"].max()
    window_start = max_ts - pd.Timedelta(days=30)

    txns_30d = txns[txns["event_timestamp"] >= window_start].copy()

    feat = dim_customer[["customer_id"]].copy()

    if len(txns_30d) > 0:
        agg = txns_30d.groupby("customer_id").agg(
            f_customer_txn_count_30d=("transaction_id", "count"),
            f_customer_txn_amount_sum_30d=("amount", "sum"),
            f_customer_avg_txn_amount_30d=("amount", "mean"),
            f_customer_max_txn_amount_30d=("amount", "max"),
            f_customer_fraud_count_30d=("is_fraud", "sum"),
            f_customer_declined_count_30d=("is_declined", "sum"),
            f_customer_foreign_ip_txn_count_30d=("is_foreign_ip", "sum"),
            f_customer_high_risk_merchant_txn_count_30d=("is_high_risk_merchant", "sum"),
            f_customer_night_txn_count_30d=("is_night_transaction", "sum"),
        ).reset_index()

        agg["f_customer_fraud_rate_30d"] = safe_divide(
            agg["f_customer_fraud_count_30d"],
            agg["f_customer_txn_count_30d"],
        )

        agg["f_customer_declined_rate_30d"] = safe_divide(
            agg["f_customer_declined_count_30d"],
            agg["f_customer_txn_count_30d"],
        )

        feat = feat.merge(agg, on="customer_id", how="left")

    numeric_cols = feat.select_dtypes(include=["number", "bool"]).columns
    feat[numeric_cols] = feat[numeric_cols].fillna(0)

    feat["feature_window_start_ts"] = window_start
    feat["feature_window_end_ts"] = max_ts
    feat["event_timestamp"] = max_ts
    feat["created_ts"] = now_utc_str()

    return add_gold_metadata(feat, batch_id)


def build_feat_transaction_realtime(
    batch_id: str,
    obt_transaction_risk: pd.DataFrame,
) -> pd.DataFrame:
    obt = obt_transaction_risk.copy()

    feat = pd.DataFrame(
        {
            "transaction_id": obt["transaction_id"],
            "customer_id": obt["customer_id"],
            "event_timestamp": obt["event_timestamp"],
            "created_ts": obt["created_ts"],
            "f_txn_amount": obt["amount"],
            "f_txn_is_foreign_ip": obt["is_foreign_ip"].astype(int),
            "f_txn_is_new_device": obt["is_new_device"].astype(int),
            "f_txn_is_night": obt["is_night_transaction"].astype(int),
            "f_txn_is_high_risk_merchant": obt["is_high_risk_merchant"].astype(int),
            "f_txn_has_alert": obt["has_alert"].astype(int),
            "f_txn_max_alert_score": obt["max_alert_score"],
            "f_txn_alert_count": obt["alert_count"],
        }
    )

    return add_gold_metadata(feat, batch_id)


def build_feat_customer_unified(
    batch_id: str,
    feat_transaction_realtime: pd.DataFrame,
    feat_customer_30d: pd.DataFrame,
) -> pd.DataFrame:
    customer_features = feat_customer_30d.drop(
        columns=[
            "event_timestamp",
            "created_ts",
            "gold_processed_ts",
            "gold_batch_id",
        ],
        errors="ignore",
    )

    unified = feat_transaction_realtime.merge(
        customer_features,
        on="customer_id",
        how="left",
    )

    numeric_cols = unified.select_dtypes(include=["number", "bool"]).columns
    unified[numeric_cols] = unified[numeric_cols].fillna(0)

    return add_gold_metadata(unified, batch_id)


def run_feature_pipelines(
    config: dict[str, Any],
    batch_id: str,
    fact_transaction: pd.DataFrame,
    dim_customer: pd.DataFrame,
    obt_transaction_risk: pd.DataFrame,
) -> tuple[list[dict[str, Any]], dict[str, pd.DataFrame]]:
    records: list[dict[str, Any]] = []

    logging.info("Starting Gold feature pipelines | batch_id=%s", batch_id)

    start_ts = now_utc_str()

    feat_customer_30d = build_feat_customer_30d(
        batch_id,
        fact_transaction,
        dim_customer,
    )

    feat_transaction_realtime = build_feat_transaction_realtime(
        batch_id,
        obt_transaction_risk,
    )

    feat_customer_unified = build_feat_customer_unified(
        batch_id,
        feat_transaction_realtime,
        feat_customer_30d,
    )

    features = {
        "feat_customer_30d": feat_customer_30d,
        "feat_transaction_realtime": feat_transaction_realtime,
        "feat_customer_unified": feat_customer_unified,
    }

    for table_name, df in features.items():
        target = write_gold_table(config, table_name, df)
        records.append(
            {
                "run_id": batch_id,
                "pipeline_name": f"gold_build_{table_name}",
                "layer": "gold_feature",
                "source": "data/gold/facts + data/gold/obt",
                "target": str(target),
                "start_ts": start_ts,
                "end_ts": now_utc_str(),
                "status": "SUCCESS",
                "input_rows": "",
                "output_rows": len(df),
                "error_count": 0,
                "error_summary": "",
            }
        )
        logging.info("Gold feature success | table=%s | output=%s", table_name, len(df))

    logging.info("Gold feature pipelines completed | batch_id=%s", batch_id)

    return records, features

# Warehouse publishing

def publish_gold_to_duckdb(config: dict[str, Any], batch_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    logging.info("Starting DuckDB warehouse publishing | batch_id=%s", batch_id)

    start_ts = now_utc_str()

    gold_path = Path(config["paths"]["gold_path"])
    warehouse_path = Path(config["paths"]["warehouse_path"])
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    summary_path = Path("reports/02_warehouse_summary.md")
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    gold_tables = [
        "dim_customer",
        "dim_account",
        "dim_card",
        "dim_merchant",
        "dim_date",
        "dim_channel",
        "fact_transaction",
        "fact_login_event",
        "fact_device_event",
        "fact_fraud_alert",
        "obt_transaction_risk",
        "obt_customer_risk_profile",
        "feat_customer_30d",
        "feat_transaction_realtime",
        "feat_customer_unified",
    ]

    conn = duckdb.connect(str(warehouse_path))

    row_counts: dict[str, int] = {}

    try:
        for table_name in gold_tables:
            parquet_path = gold_path / f"{table_name}.parquet"

            if not parquet_path.exists():
                logging.warning("Skipping missing Gold table for warehouse publish: %s", parquet_path)
                continue

            parquet_path_str = parquet_path.resolve().as_posix()

            conn.execute(
                f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT *
                FROM read_parquet('{parquet_path_str}')
                """
            )

            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            row_counts[table_name] = int(row_count)

            logging.info(
                "Published DuckDB view | table=%s | rows=%s",
                table_name,
                row_count,
            )

        fraud_by_channel = conn.execute(
            """
            SELECT
                channel,
                COUNT(*) AS txn_count,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count,
                AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_rate
            FROM obt_transaction_risk
            GROUP BY channel
            ORDER BY fraud_rate DESC
            """
        ).fetchdf()

        fraud_by_merchant_category = conn.execute(
            """
            SELECT
                merchant_category,
                COUNT(*) AS txn_count,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count,
                AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_rate
            FROM obt_transaction_risk
            GROUP BY merchant_category
            ORDER BY fraud_rate DESC
            LIMIT 10
            """
        ).fetchdf()

        high_risk_customers = conn.execute(
            """
            SELECT
                customer_id,
                customer_segment,
                customer_city,
                customer_risk_tier,
                txn_count_30d,
                fraud_count_30d,
                fraud_rate_30d,
                declined_rate_30d,
                max_alert_score_all
            FROM obt_customer_risk_profile
            ORDER BY fraud_rate_30d DESC, txn_count_30d DESC
            LIMIT 10
            """
        ).fetchdf()

        lines = []
        lines.append("# 02 Warehouse Summary")
        lines.append("")
        lines.append("## Warehouse")
        lines.append("")
        lines.append(f"- DuckDB database: `{warehouse_path}`")
        lines.append("- Gold Parquet tables are published as DuckDB views.")
        lines.append("- This DuckDB database acts as the local data warehouse / consumption layer for the mini-coursework.")
        lines.append("")

        lines.append("## Published Tables")
        lines.append("")
        lines.append("| Table | Row Count |")
        lines.append("|---|---:|")
        for table_name, count in row_counts.items():
            lines.append(f"| `{table_name}` | {count:,} |")
        lines.append("")

        lines.append("## Sample Query: Fraud Rate by Channel")
        lines.append("")
        lines.append(fraud_by_channel.to_markdown(index=False))
        lines.append("")

        lines.append("## Sample Query: Fraud Rate by Merchant Category")
        lines.append("")
        lines.append(fraud_by_merchant_category.to_markdown(index=False))
        lines.append("")

        lines.append("## Sample Query: High-Risk Customer Profiles")
        lines.append("")
        lines.append(high_risk_customers.to_markdown(index=False))
        lines.append("")

        summary_path.write_text("\n".join(lines), encoding="utf-8")

    finally:
        conn.close()

    end_ts = now_utc_str()

    records.append(
        {
            "run_id": batch_id,
            "pipeline_name": "publish_gold_to_duckdb",
            "layer": "warehouse",
            "source": str(gold_path),
            "target": str(warehouse_path),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "status": "SUCCESS",
            "input_rows": "",
            "output_rows": len(row_counts),
            "error_count": 0,
            "error_summary": "",
        }
    )

    logging.info("Wrote warehouse summary to %s", summary_path)
    logging.info("DuckDB warehouse publishing completed | batch_id=%s", batch_id)

    return records

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config")
    args = parser.parse_args()

    config = load_config(args.config)
    ensure_dirs(config)
    setup_logging(config["paths"]["log_path"])

    logging.info("Starting Section 02 pipeline")
    logging.info("Project: %s", config["project"]["name"])

    pipeline_run_id = make_batch_id()

    records: list[dict[str, Any]] = []

    bronze_records = run_bronze_ingestion(config, pipeline_run_id)
    records.extend(bronze_records)

    silver_records = run_silver_transforms(config, pipeline_run_id)
    records.extend(silver_records)

    gold_records = run_gold_models(config, pipeline_run_id)
    records.extend(gold_records)

    warehouse_records = publish_gold_to_duckdb(config, pipeline_run_id)
    records.extend(warehouse_records)

    write_run_metadata(records, config)
    write_lineage_summary(records, config)

    logging.info("Section 02 pipeline completed successfully")



if __name__ == "__main__":
    main()