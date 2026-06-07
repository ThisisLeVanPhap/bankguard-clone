from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


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
    Path(config["output"]["source_offline_path"]).mkdir(parents=True, exist_ok=True)
    Path(config["output"]["source_stream_path"]).mkdir(parents=True, exist_ok=True)
    Path(config["output"]["report_path"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["output"]["log_path"]).parent.mkdir(parents=True, exist_ok=True)


def date_range_days(start_date: str, end_date: str) -> pd.DatetimeIndex:
    return pd.date_range(start=start_date, end=end_date, freq="D")


def random_timestamps(
    rng: np.random.Generator,
    start_date: str,
    end_date: str,
    n: int,
) -> pd.Series:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    start_ns = start.value
    end_ns = end.value
    values = rng.integers(start_ns, end_ns, size=n, endpoint=False)
    return pd.to_datetime(values)


def make_ids(prefix: str, n: int, width: int = 8) -> list[str]:
    return [f"{prefix}{i:0{width}d}" for i in range(1, n + 1)]


def generate_customers(config: dict[str, Any], rng: np.random.Generator) -> pd.DataFrame:
    n = int(config["entities"]["n_customers"])

    cities = [
        "Ho Chi Minh City",
        "Ha Noi",
        "Da Nang",
        "Can Tho",
        "Hai Phong",
        "Binh Duong",
        "Dong Nai",
        "Other",
    ]
    city_probs = [0.42, 0.27, 0.08, 0.05, 0.04, 0.05, 0.04, 0.05]

    segments = ["student", "mass", "salaried", "affluent", "sme_owner"]
    segment_probs = [0.12, 0.36, 0.30, 0.12, 0.10]

    customer_segment = rng.choice(segments, size=n, p=segment_probs)

    risk_tier = []
    for seg in customer_segment:
        if seg in ["affluent", "sme_owner"]:
            risk_tier.append(rng.choice(["low", "medium", "high"], p=[0.58, 0.34, 0.08]))
        elif seg == "student":
            risk_tier.append(rng.choice(["low", "medium", "high"], p=[0.80, 0.18, 0.02]))
        else:
            risk_tier.append(rng.choice(["low", "medium", "high"], p=[0.70, 0.25, 0.05]))

    signup_ts = random_timestamps(
        rng,
        "2022-01-01",
        config["time_range"]["start_date"],
        n,
    )

    customers = pd.DataFrame(
        {
            "customer_id": make_ids("C", n),
            "full_name": [f"Customer_{i:08d}" for i in range(1, n + 1)],
            "gender": rng.choice(["M", "F", "Other"], size=n, p=[0.48, 0.50, 0.02]),
            "dob": pd.to_datetime(
                rng.integers(
                    pd.Timestamp("1965-01-01").value,
                    pd.Timestamp("2006-12-31").value,
                    size=n,
                )
            ).date,
            "city": rng.choice(cities, size=n, p=city_probs),
            "province": rng.choice(cities, size=n, p=city_probs),
            "customer_segment": customer_segment,
            "risk_tier": risk_tier,
            "signup_ts": signup_ts,
            "created_ts": signup_ts + pd.to_timedelta(rng.integers(0, 3600, size=n), unit="s"),
        }
    )

    return customers


def generate_accounts(
    customers: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    max_accounts = int(config["entities"]["max_accounts_per_customer"])

    rows = []
    account_counter = 1

    for row in customers[["customer_id", "signup_ts", "customer_segment"]].itertuples(index=False):
        if row.customer_segment == "sme_owner":
            n_accounts = rng.choice([1, 2, 3], p=[0.25, 0.45, 0.30])
        elif row.customer_segment == "affluent":
            n_accounts = rng.choice([1, 2, 3], p=[0.40, 0.42, 0.18])
        else:
            n_accounts = rng.choice(list(range(1, max_accounts + 1)), p=[0.72, 0.22, 0.06])

        for _ in range(int(n_accounts)):
            account_type = rng.choice(["savings", "current", "credit"], p=[0.62, 0.28, 0.10])
            open_ts = pd.Timestamp(row.signup_ts) + pd.Timedelta(days=int(rng.integers(0, 120)))
            status = rng.choice(["active", "frozen", "closed"], p=[0.94, 0.03, 0.03])

            close_ts = pd.NaT
            if status == "closed":
                close_ts = open_ts + pd.Timedelta(days=int(rng.integers(30, 900)))

            rows.append(
                {
                    "account_id": f"A{account_counter:08d}",
                    "customer_id": row.customer_id,
                    "account_type": account_type,
                    "account_status": status,
                    "open_ts": open_ts,
                    "close_ts": close_ts,
                    "created_ts": open_ts + pd.Timedelta(minutes=int(rng.integers(0, 120))),
                }
            )
            account_counter += 1

    return pd.DataFrame(rows)


def generate_cards(
    accounts: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    ownership_rate = float(config["entities"]["card_ownership_rate"])
    eligible = accounts.sample(frac=ownership_rate, random_state=int(rng.integers(1, 1_000_000)))

    rows = []
    for i, row in enumerate(eligible.itertuples(index=False), start=1):
        issued_ts = pd.Timestamp(row.open_ts) + pd.Timedelta(days=int(rng.integers(0, 45)))
        rows.append(
            {
                "card_id": f"CARD{i:08d}",
                "customer_id": row.customer_id,
                "account_id": row.account_id,
                "card_type": rng.choice(["debit", "credit", "prepaid"], p=[0.70, 0.25, 0.05]),
                "card_status": rng.choice(["active", "blocked", "expired"], p=[0.93, 0.04, 0.03]),
                "issued_ts": issued_ts,
                "created_ts": issued_ts + pd.Timedelta(minutes=int(rng.integers(0, 120))),
            }
        )

    return pd.DataFrame(rows)


def generate_merchants(config: dict[str, Any], rng: np.random.Generator) -> pd.DataFrame:
    n = int(config["entities"]["n_merchants"])

    categories = [
        "grocery",
        "restaurant",
        "ecommerce",
        "travel",
        "digital_goods",
        "cash_out",
        "electronics",
        "entertainment",
        "crypto",
    ]
    category_probs = [0.18, 0.17, 0.21, 0.08, 0.10, 0.08, 0.08, 0.07, 0.03]

    cities = [
        "Ho Chi Minh City",
        "Ha Noi",
        "Da Nang",
        "Can Tho",
        "Hai Phong",
        "Binh Duong",
        "Dong Nai",
        "Online",
    ]
    city_probs = [0.36, 0.25, 0.07, 0.04, 0.04, 0.05, 0.04, 0.15]

    merchant_category = rng.choice(categories, size=n, p=category_probs)
    risk_level = []
    for cat in merchant_category:
        if cat in ["crypto", "cash_out", "digital_goods"]:
            risk_level.append(rng.choice(["low", "medium", "high"], p=[0.20, 0.40, 0.40]))
        elif cat in ["ecommerce", "electronics", "travel"]:
            risk_level.append(rng.choice(["low", "medium", "high"], p=[0.45, 0.43, 0.12]))
        else:
            risk_level.append(rng.choice(["low", "medium", "high"], p=[0.78, 0.20, 0.02]))

    created_ts = random_timestamps(rng, "2021-01-01", config["time_range"]["start_date"], n)

    return pd.DataFrame(
        {
            "merchant_id": make_ids("M", n),
            "merchant_name": [f"Merchant_{i:08d}" for i in range(1, n + 1)],
            "merchant_category": merchant_category,
            "merchant_city": rng.choice(cities, size=n, p=city_probs),
            "risk_level": risk_level,
            "created_ts": created_ts,
        }
    )


def amount_for_segment(segment: str, rng: np.random.Generator) -> float:
    # Log-normal tạo phân phối lệch: nhiều giao dịch nhỏ, ít giao dịch rất lớn.
    if segment == "student":
        value = rng.lognormal(mean=np.log(120_000), sigma=0.70)
    elif segment == "mass":
        value = rng.lognormal(mean=np.log(250_000), sigma=0.80)
    elif segment == "salaried":
        value = rng.lognormal(mean=np.log(450_000), sigma=0.85)
    elif segment == "affluent":
        value = rng.lognormal(mean=np.log(1_200_000), sigma=0.90)
    else:  # sme_owner
        value = rng.lognormal(mean=np.log(1_800_000), sigma=1.00)

    # Làm tròn nghìn VND
    return float(round(value / 1000) * 1000)


def compute_fraud_probability(
    base_rate: float,
    amount: float,
    segment: str,
    merchant_risk: str,
    channel: str,
    ip_country: str,
    is_new_device: bool,
    event_ts: pd.Timestamp,
) -> tuple[float, list[str]]:
    prob = base_rate
    reasons = []

    high_amount_threshold = {
        "student": 1_000_000,
        "mass": 2_500_000,
        "salaried": 5_000_000,
        "affluent": 15_000_000,
        "sme_owner": 25_000_000,
    }[segment]

    if merchant_risk == "high":
        prob += 0.018
        reasons.append("high_risk_merchant")

    if merchant_risk == "medium":
        prob += 0.006

    if amount > high_amount_threshold:
        prob += 0.020
        reasons.append("high_amount")

    if channel in ["online", "mobile"]:
        prob += 0.002

    if ip_country != "VN":
        prob += 0.020
        reasons.append("foreign_ip")

    if is_new_device:
        prob += 0.015
        reasons.append("new_device")

    if event_ts.hour < 5:
        prob += 0.006
        reasons.append("night_transaction")

    return min(prob, 0.35), reasons


def generate_transactions(
    customers: pd.DataFrame,
    accounts: pd.DataFrame,
    cards: pd.DataFrame,
    merchants: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    start_date = config["time_range"]["start_date"]
    end_date = config["time_range"]["end_date"]

    n_customers = len(customers)
    months = max(1, len(date_range_days(start_date, end_date)) / 30)
    avg_txn = float(config["behavior"]["avg_transactions_per_customer_per_month"])
    n_txn = int(n_customers * avg_txn * months)

    logging.info("Generating historical transactions: %s rows", n_txn)

    customer_lookup = customers.set_index("customer_id")[["customer_segment", "city", "risk_tier"]].to_dict("index")

    accounts_by_customer = accounts.groupby("customer_id")["account_id"].apply(list).to_dict()
    cards_by_customer = cards.groupby("customer_id")["card_id"].apply(list).to_dict()

    merchant_lookup = merchants.set_index("merchant_id")[["merchant_category", "merchant_city", "risk_level"]].to_dict("index")

    customer_ids = customers["customer_id"].to_numpy()
    merchant_ids = merchants["merchant_id"].to_numpy()

    # Weight theo segment để sme/affluent có nhiều transaction hơn.
    segment_weight = customers["customer_segment"].map(
        {
            "student": 0.55,
            "mass": 1.00,
            "salaried": 1.15,
            "affluent": 1.40,
            "sme_owner": 1.75,
        }
    ).to_numpy()
    customer_probs = segment_weight / segment_weight.sum()

    event_ts_values = random_timestamps(rng, start_date, end_date, n_txn)

    rows = []
    base_fraud_rate = float(config["behavior"]["base_fraud_rate"])
    late_rate = float(config["data_quality"]["late_arrival_rate"])
    late_min = int(config["data_quality"]["late_arrival_min_minutes"])
    late_max = int(config["data_quality"]["late_arrival_max_minutes"])

    for i in range(n_txn):
        customer_id = rng.choice(customer_ids, p=customer_probs)
        c = customer_lookup[customer_id]

        account_id = rng.choice(accounts_by_customer[customer_id])
        card_list = cards_by_customer.get(customer_id, [])
        card_id = rng.choice(card_list) if len(card_list) > 0 and rng.random() < 0.82 else None

        merchant_id = rng.choice(merchant_ids)
        m = merchant_lookup[merchant_id]

        event_ts = pd.Timestamp(event_ts_values[i])
        amount = amount_for_segment(c["customer_segment"], rng)

        channel = rng.choice(["mobile", "web", "atm", "pos", "online"], p=[0.35, 0.14, 0.10, 0.26, 0.15])

        # Foreign IP chủ yếu online/mobile/web.
        if channel in ["mobile", "web", "online"]:
            ip_country = rng.choice(["VN", "SG", "TH", "US", "CN", "Other"], p=[0.91, 0.025, 0.02, 0.02, 0.015, 0.01])
        else:
            ip_country = "VN"

        is_new_device = bool(rng.random() < 0.08)
        device_id = f"D{rng.integers(1, int(n_customers * 1.8)):08d}" if channel in ["mobile", "web", "online"] else None

        fraud_prob, reasons = compute_fraud_probability(
            base_rate=base_fraud_rate,
            amount=amount,
            segment=c["customer_segment"],
            merchant_risk=m["risk_level"],
            channel=channel,
            ip_country=ip_country,
            is_new_device=is_new_device,
            event_ts=event_ts,
        )

        is_fraud = bool(rng.random() < fraud_prob)

        if is_fraud:
            transaction_status = rng.choice(["authorized", "settled", "declined"], p=[0.38, 0.34, 0.28])
        else:
            transaction_status = rng.choice(["authorized", "settled", "declined"], p=[0.20, 0.76, 0.04])

        if rng.random() < late_rate:
            created_ts = event_ts + pd.Timedelta(minutes=int(rng.integers(late_min, late_max + 1)))
            is_late_arrival = True
        else:
            created_ts = event_ts + pd.Timedelta(seconds=int(rng.integers(1, 180)))
            is_late_arrival = False

        rows.append(
            {
                "transaction_id": f"T{i + 1:010d}",
                "customer_id": customer_id,
                "account_id": account_id,
                "card_id": card_id,
                "merchant_id": merchant_id,
                "event_timestamp": event_ts,
                "created_ts": created_ts,
                "amount": amount,
                "currency": config["behavior"]["currency"],
                "channel": channel,
                "transaction_type": rng.choice(["purchase", "transfer", "cash_withdrawal"], p=[0.78, 0.16, 0.06]),
                "transaction_status": transaction_status,
                "device_id": device_id,
                "ip_country": ip_country,
                "is_new_device": is_new_device,
                "is_late_arrival": is_late_arrival,
                "merchant_category": m["merchant_category"],
                "merchant_risk_level": m["risk_level"],
                "is_fraud": is_fraud,
                "fraud_reason_codes": ",".join(reasons),
            }
        )

    txns = pd.DataFrame(rows)

    txns = apply_schema_evolution(txns, config)
    txns = inject_transaction_quality_issues(txns, config, rng)

    return txns


def apply_schema_evolution(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    schema_change_date = pd.Timestamp(config["schema_evolution"]["schema_change_date"])
    old_cols = config["schema_evolution"]["old_missing_columns"]

    old_mask = df["event_timestamp"] < schema_change_date
    for col in old_cols:
        if col in df.columns:
            df.loc[old_mask, col] = pd.NA

    df["schema_version"] = np.where(old_mask, "v1_missing_device_ip_channel", "v2_full_schema")
    return df


def inject_transaction_quality_issues(
    df: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    missing_device_rate = float(config["data_quality"]["missing_device_id_rate"])
    missing_merchant_rate = float(config["data_quality"]["missing_merchant_category_rate"])
    duplicate_rate = float(config["data_quality"]["duplicate_transaction_rate"])

    # Missing device_id, ngoài schema evolution.
    mask_device = rng.random(len(df)) < missing_device_rate
    df.loc[mask_device, "device_id"] = pd.NA

    # Missing merchant_category.
    mask_merchant = rng.random(len(df)) < missing_merchant_rate
    df.loc[mask_merchant, "merchant_category"] = pd.NA

    # Duplicate transaction rows.
    n_dup = int(len(df) * duplicate_rate)
    if n_dup > 0:
        dup = df.sample(n=n_dup, random_state=int(rng.integers(1, 1_000_000))).copy()
        dup["created_ts"] = pd.to_datetime(dup["created_ts"]) + pd.to_timedelta(
            rng.integers(1, 20, size=len(dup)), unit="m"
        )
        df = pd.concat([df, dup], ignore_index=True)

    return df


def generate_fraud_cases(transactions: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    fraud_txns = transactions[transactions["is_fraud"] == True].drop_duplicates("transaction_id").copy()

    fraud_types = ["stolen_card", "account_takeover", "merchant_fraud", "phishing", "cash_out_abuse"]

    rows = []
    for i, row in enumerate(fraud_txns.itertuples(index=False), start=1):
        event_ts = pd.Timestamp(row.event_timestamp)
        case_open_ts = event_ts + pd.Timedelta(hours=int(rng.integers(1, 72)))
        rows.append(
            {
                "case_id": f"FC{i:09d}",
                "transaction_id": row.transaction_id,
                "customer_id": row.customer_id,
                "case_open_ts": case_open_ts,
                "case_status": rng.choice(["confirmed", "under_review", "rejected"], p=[0.70, 0.22, 0.08]),
                "fraud_type": rng.choice(fraud_types, p=[0.25, 0.22, 0.18, 0.22, 0.13]),
                "label_created_ts": case_open_ts + pd.Timedelta(hours=int(rng.integers(1, 48))),
            }
        )

    return pd.DataFrame(rows)


def generate_transaction_events(
    transactions: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    cols = [
        "transaction_id",
        "customer_id",
        "account_id",
        "card_id",
        "merchant_id",
        "event_timestamp",
        "created_ts",
        "amount",
        "channel",
        "device_id",
        "ip_country",
        "transaction_status",
        "is_fraud",
    ]

    events = transactions[cols].copy()
    events = events.drop_duplicates("transaction_id")

    events.insert(0, "event_id", [f"EVT_TXN_{i:010d}" for i in range(1, len(events) + 1)])
    events.insert(1, "event_type", np.where(events["transaction_status"] == "declined", "transaction_declined", "transaction_authorized"))
    events["event_status"] = events["transaction_status"]

    duplicate_rate = float(config["data_quality"]["duplicate_event_rate"])
    n_dup = int(len(events) * duplicate_rate)
    if n_dup > 0:
        dup = events.sample(n=n_dup, random_state=int(rng.integers(1, 1_000_000))).copy()
        dup["created_ts"] = pd.to_datetime(dup["created_ts"]) + pd.to_timedelta(
            rng.integers(1, 4, size=len(dup)), unit="m"
        )
        events = pd.concat([events, dup], ignore_index=True)

    return maybe_out_of_order(events, config, rng)


def generate_login_events(
    customers: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    n = int(len(customers) * 8)
    customer_ids = customers["customer_id"].to_numpy()
    event_ts = random_timestamps(rng, config["time_range"]["start_date"], config["time_range"]["end_date"], n)

    status = rng.choice(["success", "failed"], size=n, p=[0.86, 0.14])
    failure_reason = np.where(
        status == "failed",
        rng.choice(["wrong_password", "otp_failed", "device_blocked", "risk_blocked"], size=n, p=[0.55, 0.25, 0.12, 0.08]),
        None,
    )

    events = pd.DataFrame(
        {
            "event_id": [f"EVT_LOGIN_{i:010d}" for i in range(1, n + 1)],
            "event_type": np.where(status == "success", "login_success", "login_failed"),
            "event_timestamp": event_ts,
            "created_ts": event_ts + pd.to_timedelta(rng.integers(1, 240, size=n), unit="s"),
            "customer_id": rng.choice(customer_ids, size=n),
            "device_id": [f"D{x:08d}" for x in rng.integers(1, int(len(customers) * 1.8), size=n)],
            "ip_country": rng.choice(["VN", "SG", "TH", "US", "CN", "Other"], size=n, p=[0.93, 0.02, 0.015, 0.015, 0.01, 0.01]),
            "login_status": status,
            "failure_reason": failure_reason,
        }
    )

    return maybe_out_of_order(events, config, rng)


def generate_device_events(
    customers: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    n = int(len(customers) * 1.5)
    customer_ids = customers["customer_id"].to_numpy()
    event_ts = random_timestamps(rng, config["time_range"]["start_date"], config["time_range"]["end_date"], n)

    events = pd.DataFrame(
        {
            "event_id": [f"EVT_DEVICE_{i:010d}" for i in range(1, n + 1)],
            "event_type": rng.choice(["device_registered", "device_changed", "device_removed"], size=n, p=[0.65, 0.25, 0.10]),
            "event_timestamp": event_ts,
            "created_ts": event_ts + pd.to_timedelta(rng.integers(1, 600, size=n), unit="s"),
            "customer_id": rng.choice(customer_ids, size=n),
            "device_id": [f"D{x:08d}" for x in rng.integers(1, int(len(customers) * 1.8), size=n)],
            "device_type": rng.choice(["ios", "android", "web_browser"], size=n, p=[0.34, 0.52, 0.14]),
            "os": rng.choice(["iOS", "Android", "Windows", "macOS", "Linux"], size=n, p=[0.30, 0.50, 0.10, 0.06, 0.04]),
            "is_new_device": rng.choice([True, False], size=n, p=[0.70, 0.30]),
        }
    )

    return maybe_out_of_order(events, config, rng)


def generate_fraud_alert_events(
    transactions: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    base = transactions.drop_duplicates("transaction_id").copy()

    suspicious_mask = (
        (base["is_fraud"] == True)
        | (base["merchant_risk_level"] == "high")
        | (base["ip_country"].fillna("VN") != "VN")
        | (base["is_new_device"] == True)
        | (base["amount"] > base["amount"].quantile(0.97))
    )

    alert_txns = base[suspicious_mask].copy()
    if len(alert_txns) == 0:
        return pd.DataFrame()

    alert_rules = []
    alert_scores = []

    for row in alert_txns.itertuples(index=False):
        rules = []
        score = 0.15

        if row.merchant_risk_level == "high":
            rules.append("high_risk_merchant")
            score += 0.25
        if pd.notna(row.ip_country) and row.ip_country != "VN":
            rules.append("foreign_ip")
            score += 0.25
        if row.is_new_device:
            rules.append("new_device")
            score += 0.20
        if row.amount > base["amount"].quantile(0.97):
            rules.append("high_amount")
            score += 0.20
        if pd.Timestamp(row.event_timestamp).hour < 5:
            rules.append("night_transaction")
            score += 0.10
        if row.is_fraud:
            score += 0.20

        alert_rules.append(",".join(rules) if rules else "generic_risk_rule")
        alert_scores.append(round(min(score, 0.99), 4))

    alerts = pd.DataFrame(
        {
            "event_id": [f"EVT_ALERT_{i:010d}" for i in range(1, len(alert_txns) + 1)],
            "event_type": "fraud_rule_alert",
            "event_timestamp": pd.to_datetime(alert_txns["event_timestamp"]) + pd.to_timedelta(
                rng.integers(1, 180, size=len(alert_txns)), unit="s"
            ),
            "created_ts": pd.to_datetime(alert_txns["created_ts"]) + pd.to_timedelta(
                rng.integers(1, 300, size=len(alert_txns)), unit="s"
            ),
            "transaction_id": alert_txns["transaction_id"].to_numpy(),
            "customer_id": alert_txns["customer_id"].to_numpy(),
            "alert_rule": alert_rules,
            "alert_score": alert_scores,
            "alert_status": rng.choice(["open", "closed", "suppressed"], size=len(alert_txns), p=[0.55, 0.35, 0.10]),
        }
    )

    return maybe_out_of_order(alerts, config, rng)


def maybe_out_of_order(
    events: pd.DataFrame,
    config: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    out_of_order_rate = float(config["data_quality"]["out_of_order_event_rate"])

    events = events.sort_values("event_timestamp").reset_index(drop=True)

    if len(events) == 0:
        return events

    n_shuffle = int(len(events) * out_of_order_rate)
    if n_shuffle <= 1:
        return events

    idx = rng.choice(events.index.to_numpy(), size=n_shuffle, replace=False)
    shuffled = events.loc[idx].sample(frac=1.0, random_state=int(rng.integers(1, 1_000_000)))
    events.loc[idx] = shuffled.to_numpy()

    return events.reset_index(drop=True)


def write_jsonl(df: pd.DataFrame, path: Path) -> None:
    # JSONL mô phỏng event stream.
    with path.open("w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            clean = {}
            for k, v in record.items():
                if pd.isna(v):
                    clean[k] = None
                elif isinstance(v, pd.Timestamp):
                    clean[k] = v.isoformat()
                else:
                    clean[k] = v
            f.write(json.dumps(clean, ensure_ascii=False) + "\n")


def write_outputs(
    datasets: dict[str, pd.DataFrame],
    config: dict[str, Any],
) -> None:
    offline_path = Path(config["output"]["source_offline_path"])
    stream_path = Path(config["output"]["source_stream_path"])

    offline_tables = [
        "customers",
        "accounts",
        "cards",
        "merchants",
        "historical_transactions",
        "fraud_cases",
    ]

    stream_tables = [
        "transaction_events",
        "login_events",
        "device_events",
        "fraud_alert_events",
    ]

    for name in offline_tables:
        path = offline_path / f"{name}.parquet"
        datasets[name].to_parquet(path, index=False)
        logging.info("Wrote %s rows to %s", len(datasets[name]), path)

    for name in stream_tables:
        path = stream_path / f"{name}.jsonl"
        write_jsonl(datasets[name], path)
        logging.info("Wrote %s rows to %s", len(datasets[name]), path)


def write_generation_report(
    datasets: dict[str, pd.DataFrame],
    config: dict[str, Any],
) -> None:
    report_path = Path(config["output"]["report_path"])
    txns = datasets["historical_transactions"]
    customers = datasets["customers"]
    merchants = datasets["merchants"]
    transaction_events = datasets["transaction_events"]

    txn_unique = txns.drop_duplicates("transaction_id")
    fraud_rate = txn_unique["is_fraud"].mean()
    duplicate_txn_rate = 1 - txns["transaction_id"].nunique() / len(txns)
    duplicate_event_rate = 1 - transaction_events["event_id"].nunique() / len(transaction_events)

    late_txn_rate = txns["is_late_arrival"].mean()
    missing_device_rate = txns["device_id"].isna().mean()
    missing_merchant_cat_rate = txns["merchant_category"].isna().mean()

    schema_change_date = pd.Timestamp(config["schema_evolution"]["schema_change_date"])
    old_records = txns[txns["event_timestamp"] < schema_change_date]

    city_dist = customers["city"].value_counts(normalize=True).head(10)
    merchant_cat_dist = merchants["merchant_category"].value_counts(normalize=True).head(10)

    lines = []
    lines.append("# 01 Data Generation Quality Report\n")
    lines.append("## Project\n")
    lines.append(f"- Project name: `{config['project']['name']}`")
    lines.append("- Data source strategy: synthetic source-system simulator")
    lines.append("- Offline output: Parquet")
    lines.append("- Streaming output: JSONL")
    lines.append("")

    lines.append("## Row Counts\n")
    for name, df in datasets.items():
        lines.append(f"- `{name}`: {len(df):,} rows")
    lines.append("")

    lines.append("## Core Quality Metrics\n")
    lines.append(f"- Unique transaction count: {txns['transaction_id'].nunique():,}")
    lines.append(f"- Fraud rate on unique transactions: {fraud_rate:.4%}")
    lines.append(f"- Duplicate transaction row rate: {duplicate_txn_rate:.4%}")
    lines.append(f"- Duplicate stream event rate: {duplicate_event_rate:.4%}")
    lines.append(f"- Late-arriving transaction rate: {late_txn_rate:.4%}")
    lines.append(f"- Missing `device_id` rate: {missing_device_rate:.4%}")
    lines.append(f"- Missing `merchant_category` rate: {missing_merchant_cat_rate:.4%}")
    lines.append("")

    lines.append("## Timestamp Checks\n")
    lines.append(f"- Transactions where `created_ts >= event_timestamp`: {(pd.to_datetime(txns['created_ts']) >= pd.to_datetime(txns['event_timestamp'])).mean():.4%}")
    lines.append(f"- Transaction event time range: {txns['event_timestamp'].min()} to {txns['event_timestamp'].max()}")
    lines.append(f"- Transaction created time range: {txns['created_ts'].min()} to {txns['created_ts'].max()}")
    lines.append("")

    lines.append("## Schema Evolution Check\n")
    lines.append(f"- Schema change date: `{schema_change_date.date()}`")
    lines.append(f"- Old records before schema change: {len(old_records):,}")
    for col in config["schema_evolution"]["old_missing_columns"]:
        if col in txns.columns and len(old_records) > 0:
            lines.append(f"- Old records missing `{col}`: {old_records[col].isna().mean():.4%}")
    lines.append("")

    lines.append("## Skew Checks\n")
    lines.append("### Customer city distribution\n")
    for city, pct in city_dist.items():
        lines.append(f"- {city}: {pct:.2%}")

    lines.append("\n### Merchant category distribution\n")
    for cat, pct in merchant_cat_dist.items():
        lines.append(f"- {cat}: {pct:.2%}")

    lines.append("\n## Cardinality Checks\n")
    cardinality_cols = [
        ("customers", "customer_id"),
        ("accounts", "account_id"),
        ("cards", "card_id"),
        ("merchants", "merchant_id"),
        ("historical_transactions", "transaction_id"),
        ("transaction_events", "event_id"),
    ]
    for table, col in cardinality_cols:
        df = datasets[table]
        lines.append(f"- `{table}.{col}` distinct count: {df[col].nunique():,}")

    lines.append("\n## Data Quality Challenges Injected\n")
    lines.append("- Skewed city and merchant category distributions")
    lines.append("- High-cardinality customer, transaction, device, merchant identifiers")
    lines.append("- Duplicate transaction rows")
    lines.append("- Duplicate streaming events")
    lines.append("- Late-arriving events using `event_timestamp` vs `created_ts`")
    lines.append("- Missing `device_id` and `merchant_category`")
    lines.append("- Out-of-order streaming events")
    lines.append("- Schema evolution before configured schema change date")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    logging.info("Wrote generation report to %s", report_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to generator YAML config")
    args = parser.parse_args()

    config = load_config(args.config)
    ensure_dirs(config)
    setup_logging(config["output"]["log_path"])

    seed = int(config["project"]["random_seed"])
    rng = np.random.default_rng(seed)

    logging.info("Starting data generation")
    logging.info("Using random seed: %s", seed)

    customers = generate_customers(config, rng)
    accounts = generate_accounts(customers, config, rng)
    cards = generate_cards(accounts, config, rng)
    merchants = generate_merchants(config, rng)
    historical_transactions = generate_transactions(customers, accounts, cards, merchants, config, rng)
    fraud_cases = generate_fraud_cases(historical_transactions, rng)

    transaction_events = generate_transaction_events(historical_transactions, config, rng)
    login_events = generate_login_events(customers, config, rng)
    device_events = generate_device_events(customers, config, rng)
    fraud_alert_events = generate_fraud_alert_events(historical_transactions, config, rng)

    datasets = {
        "customers": customers,
        "accounts": accounts,
        "cards": cards,
        "merchants": merchants,
        "historical_transactions": historical_transactions,
        "fraud_cases": fraud_cases,
        "transaction_events": transaction_events,
        "login_events": login_events,
        "device_events": device_events,
        "fraud_alert_events": fraud_alert_events,
    }

    write_outputs(datasets, config)
    write_generation_report(datasets, config)

    logging.info("Data generation completed successfully")


if __name__ == "__main__":
    main()