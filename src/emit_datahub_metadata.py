from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    AuditStampClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    TagAssociationClass,
)


ACTOR = "urn:li:corpuser:coursework"


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_audit_stamp() -> AuditStampClass:
    return AuditStampClass(
        time=0,
        actor=ACTOR,
    )


def infer_datahub_type(dtype: Any):
    dtype_str = str(dtype).lower()

    if "datetime" in dtype_str or "date" in dtype_str or "timestamp" in dtype_str:
        return TimeTypeClass()

    if (
        "int" in dtype_str
        or "float" in dtype_str
        or "double" in dtype_str
        or "decimal" in dtype_str
    ):
        return NumberTypeClass()

    if "bool" in dtype_str:
        return BooleanTypeClass()

    return StringTypeClass()


def dataset_urn(platform: str, name: str, env: str = "DEV") -> str:
    return make_dataset_urn(platform=platform, name=name, env=env)


def emit_dataset_properties(
    emitter: DatahubRestEmitter,
    urn: str,
    name: str,
    description: str,
    tags: list[str],
) -> None:
    props = DatasetPropertiesClass(
        name=name,
        description=description,
        customProperties={
            "coursework_domain": "digital_banking_fraud_monitoring",
            "environment": "local",
        },
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=props,
        )
    )

    tag_aspects = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag=f"urn:li:tag:{tag}")
            for tag in tags
        ]
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=tag_aspects,
        )
    )


def emit_dataset_ownership(
    emitter: DatahubRestEmitter,
    urn: str,
) -> None:
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=ACTOR,
                type=OwnershipTypeClass.DATAOWNER,
            )
        ]
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ownership,
        )
    )


def emit_schema_from_dataframe(
    emitter: DatahubRestEmitter,
    urn: str,
    df: pd.DataFrame,
) -> None:
    fields = []

    for col in df.columns:
        fields.append(
            SchemaFieldClass(
                fieldPath=col,
                type=SchemaFieldDataTypeClass(
                    type=infer_datahub_type(df[col].dtype)
                ),
                nativeDataType=str(df[col].dtype),
                description=describe_column(col),
                nullable=True,
                recursive=False,
            )
        )

    schema = SchemaMetadataClass(
        schemaName=urn.split(",")[1] if "," in urn else urn,
        platform="urn:li:dataPlatform:delta",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=schema,
        )
    )


def describe_column(col: str) -> str:
    descriptions = {
        "customer_id": "Business identifier of the banking customer.",
        "account_id": "Business identifier of the bank account.",
        "card_id": "Business identifier of the card.",
        "merchant_id": "Business identifier of the merchant.",
        "transaction_id": "Business identifier of the transaction.",
        "event_id": "Unique identifier of a streaming event.",
        "event_timestamp": "Business event time when the event occurred.",
        "created_ts": "Record creation or availability timestamp used for ingestion and late-arrival simulation.",
        "ingest_ts": "Timestamp when the record was ingested into the Bronze layer.",
        "batch_id": "Pipeline run or ingestion batch identifier.",
        "amount": "Transaction amount in VND.",
        "channel": "Transaction channel such as mobile, web, atm, pos, online, or unknown.",
        "device_id": "Identifier of the device used for digital banking events.",
        "ip_country": "Country inferred from IP address.",
        "is_fraud": "Synthetic fraud label generated by the source simulator.",
        "is_late_arrival": "Whether created_ts is later than event_timestamp by a configured delay.",
        "merchant_category": "Business category of the merchant.",
        "merchant_risk_level": "Risk level of the merchant in the transaction record.",
        "customer_key": "Gold surrogate key for customer dimension.",
        "account_key": "Gold surrogate key for account dimension.",
        "card_key": "Gold surrogate key for card dimension.",
        "merchant_key": "Gold surrogate key for merchant dimension.",
        "date_key": "Date dimension key in YYYYMMDD format.",
        "channel_key": "Gold surrogate key for channel dimension.",
        "is_declined": "Whether the transaction status is declined.",
        "is_foreign_ip": "Whether the IP country is outside Vietnam.",
        "is_high_risk_merchant": "Whether the transaction is linked to a high-risk merchant.",
        "is_night_transaction": "Whether the transaction occurred during night hours.",
        "has_alert": "Whether at least one fraud rule alert exists for the transaction.",
        "alert_count": "Number of fraud rule alerts linked to the transaction.",
        "max_alert_score": "Maximum alert score linked to the transaction.",
    }

    return descriptions.get(col, f"Column `{col}` in the digital banking fraud monitoring dataset.")


def emit_upstream_lineage(
    emitter: DatahubRestEmitter,
    downstream_urn: str,
    upstream_urns: list[str],
) -> None:
    lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            for upstream in upstream_urns
        ]
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=lineage,
        )
    )


def emit_data_flow(
    emitter: DatahubRestEmitter,
    flow_urn: str,
) -> None:
    flow_info = DataFlowInfoClass(
        name="digital_banking_fraud_monitoring_pipeline",
        description="Coursework pipeline for digital banking fraud monitoring: source to Bronze, Silver, Gold, features, and DuckDB warehouse.",
        project="fsds_coursework",
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=flow_info,
        )
    )


def emit_data_job(
    emitter: DatahubRestEmitter,
    job_urn: str,
    name: str,
    description: str,
    input_urns: list[str],
    output_urns: list[str],
) -> None:
    job_info = DataJobInfoClass(
        name=name,
        description=description,
        type="BATCH",
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=job_info,
        )
    )

    io = DataJobInputOutputClass(
        inputDatasets=input_urns,
        outputDatasets=output_urns,
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=io,
        )
    )


def read_gold_table(gold_path: Path, table_name: str) -> pd.DataFrame | None:
    path = gold_path / f"{table_name}.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


def safe_read_delta_table(path: Path) -> pd.DataFrame | None:
    try:
        from deltalake import DeltaTable

        if not path.exists():
            return None

        return DeltaTable(str(path)).to_pandas()
    except Exception:
        return None


def emit_datasets(
    emitter: DatahubRestEmitter,
    config: dict[str, Any],
) -> dict[str, str]:
    bronze_path = Path(config["paths"]["bronze_path"])
    silver_path = Path(config["paths"]["silver_path"])
    gold_path = Path(config["paths"]["gold_path"])

    dataset_map: dict[str, str] = {}

    bronze_tables = [
        "raw_customers",
        "raw_accounts",
        "raw_cards",
        "raw_merchants",
        "raw_historical_transactions",
        "raw_fraud_cases",
        "raw_transaction_events",
        "raw_login_events",
        "raw_device_events",
        "raw_fraud_alert_events",
    ]

    silver_tables = [
        "stg_customers",
        "stg_accounts",
        "stg_cards",
        "stg_merchants",
        "stg_transactions",
        "stg_fraud_cases",
        "stg_transaction_events",
        "stg_login_events",
        "stg_device_events",
        "stg_fraud_alert_events",
    ]

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

    for table in bronze_tables:
        urn = dataset_urn("delta", f"finance.bronze.{table}")
        dataset_map[f"bronze.{table}"] = urn

        emit_dataset_properties(
            emitter,
            urn,
            table,
            f"Bronze Delta Lake table `{table}` generated by ingestion from source-system simulator.",
            ["bronze", "delta", "banking", "fraud-monitoring"],
        )
        emit_dataset_ownership(emitter, urn)

        df = safe_read_delta_table(bronze_path / table)
        if df is not None:
            emit_schema_from_dataframe(emitter, urn, df.head(1000))

    for table in silver_tables:
        urn = dataset_urn("delta", f"finance.silver.{table}")
        dataset_map[f"silver.{table}"] = urn

        emit_dataset_properties(
            emitter,
            urn,
            table,
            f"Silver Delta Lake table `{table}` after deduplication, standardization, and schema handling.",
            ["silver", "delta", "cleaned", "banking", "fraud-monitoring"],
        )
        emit_dataset_ownership(emitter, urn)

        df = safe_read_delta_table(silver_path / table)
        if df is not None:
            emit_schema_from_dataframe(emitter, urn, df.head(1000))

    for table in gold_tables:
        urn = dataset_urn("duckdb", f"finance.gold.{table}")
        dataset_map[f"gold.{table}"] = urn

        emit_dataset_properties(
            emitter,
            urn,
            table,
            f"Gold business-ready dataset `{table}` for fraud analytics, monitoring, or AI features.",
            ["gold", "duckdb", "serving", "banking", "fraud-monitoring"],
        )
        emit_dataset_ownership(emitter, urn)

        df = read_gold_table(gold_path, table)
        if df is not None:
            emit_schema_from_dataframe(emitter, urn, df.head(1000))

    warehouse_urn = dataset_urn("duckdb", "finance.warehouse.finance_duckdb")
    dataset_map["warehouse.finance_duckdb"] = warehouse_urn

    emit_dataset_properties(
        emitter,
        warehouse_urn,
        "finance_duckdb",
        "Local DuckDB warehouse that publishes Gold tables as queryable views for analytics and consumption.",
        ["warehouse", "duckdb", "consumption", "banking"],
    )
    emit_dataset_ownership(emitter, warehouse_urn)

    return dataset_map


def emit_dataset_lineage(
    emitter: DatahubRestEmitter,
    dataset_map: dict[str, str],
) -> None:
    # Bronze -> Silver
    bronze_to_silver = {
        "silver.stg_customers": ["bronze.raw_customers"],
        "silver.stg_accounts": ["bronze.raw_accounts"],
        "silver.stg_cards": ["bronze.raw_cards"],
        "silver.stg_merchants": ["bronze.raw_merchants"],
        "silver.stg_transactions": ["bronze.raw_historical_transactions"],
        "silver.stg_fraud_cases": ["bronze.raw_fraud_cases"],
        "silver.stg_transaction_events": ["bronze.raw_transaction_events"],
        "silver.stg_login_events": ["bronze.raw_login_events"],
        "silver.stg_device_events": ["bronze.raw_device_events"],
        "silver.stg_fraud_alert_events": ["bronze.raw_fraud_alert_events"],
    }

    # Silver -> Gold
    silver_to_gold = {
        "gold.dim_customer": ["silver.stg_customers"],
        "gold.dim_account": ["silver.stg_accounts"],
        "gold.dim_card": ["silver.stg_cards"],
        "gold.dim_merchant": ["silver.stg_merchants"],
        "gold.dim_date": ["silver.stg_transactions"],
        "gold.dim_channel": ["silver.stg_transactions"],
        "gold.fact_transaction": [
            "silver.stg_transactions",
            "gold.dim_customer",
            "gold.dim_account",
            "gold.dim_card",
            "gold.dim_merchant",
            "gold.dim_date",
            "gold.dim_channel",
        ],
        "gold.fact_login_event": ["silver.stg_login_events", "gold.dim_customer", "gold.dim_date"],
        "gold.fact_device_event": ["silver.stg_device_events", "gold.dim_customer", "gold.dim_date"],
        "gold.fact_fraud_alert": ["silver.stg_fraud_alert_events", "gold.dim_customer", "gold.dim_date"],
        "gold.obt_transaction_risk": [
            "gold.fact_transaction",
            "gold.fact_fraud_alert",
            "gold.dim_customer",
            "gold.dim_account",
            "gold.dim_card",
            "gold.dim_merchant",
            "gold.dim_channel",
        ],
        "gold.obt_customer_risk_profile": [
            "gold.fact_transaction",
            "gold.fact_login_event",
            "gold.fact_device_event",
            "gold.fact_fraud_alert",
            "gold.dim_customer",
        ],
        "gold.feat_customer_30d": ["gold.fact_transaction", "gold.dim_customer"],
        "gold.feat_transaction_realtime": ["gold.obt_transaction_risk"],
        "gold.feat_customer_unified": [
            "gold.feat_transaction_realtime",
            "gold.feat_customer_30d",
        ],
        "warehouse.finance_duckdb": [
            "gold.dim_customer",
            "gold.dim_account",
            "gold.dim_card",
            "gold.dim_merchant",
            "gold.dim_date",
            "gold.dim_channel",
            "gold.fact_transaction",
            "gold.fact_login_event",
            "gold.fact_device_event",
            "gold.fact_fraud_alert",
            "gold.obt_transaction_risk",
            "gold.obt_customer_risk_profile",
            "gold.feat_customer_30d",
            "gold.feat_transaction_realtime",
            "gold.feat_customer_unified",
        ],
    }

    lineage = {}
    lineage.update(bronze_to_silver)
    lineage.update(silver_to_gold)

    for downstream, upstreams in lineage.items():
        if downstream not in dataset_map:
            continue

        upstream_urns = [
            dataset_map[u]
            for u in upstreams
            if u in dataset_map
        ]

        if upstream_urns:
            emit_upstream_lineage(
                emitter,
                dataset_map[downstream],
                upstream_urns,
            )


def emit_pipeline_jobs(
    emitter: DatahubRestEmitter,
    dataset_map: dict[str, str],
) -> None:
    flow_urn = make_data_flow_urn(
        orchestrator="airflow",
        flow_id="digital_banking_fraud_monitoring_pipeline",
        cluster="local",
    )

    emit_data_flow(emitter, flow_urn)

    jobs = [
        {
            "job_id": "bronze_ingest",
            "name": "bronze_ingest",
            "description": "Ingest source-system offline and streaming files into Bronze Delta Lake tables.",
            "inputs": [],
            "outputs": [
                "bronze.raw_customers",
                "bronze.raw_accounts",
                "bronze.raw_cards",
                "bronze.raw_merchants",
                "bronze.raw_historical_transactions",
                "bronze.raw_fraud_cases",
                "bronze.raw_transaction_events",
                "bronze.raw_login_events",
                "bronze.raw_device_events",
                "bronze.raw_fraud_alert_events",
            ],
        },
        {
            "job_id": "silver_transform",
            "name": "silver_transform",
            "description": "Clean, deduplicate, standardize timestamps, and handle schema evolution in Silver Delta tables.",
            "inputs": [
                "bronze.raw_customers",
                "bronze.raw_accounts",
                "bronze.raw_cards",
                "bronze.raw_merchants",
                "bronze.raw_historical_transactions",
                "bronze.raw_fraud_cases",
                "bronze.raw_transaction_events",
                "bronze.raw_login_events",
                "bronze.raw_device_events",
                "bronze.raw_fraud_alert_events",
            ],
            "outputs": [
                "silver.stg_customers",
                "silver.stg_accounts",
                "silver.stg_cards",
                "silver.stg_merchants",
                "silver.stg_transactions",
                "silver.stg_fraud_cases",
                "silver.stg_transaction_events",
                "silver.stg_login_events",
                "silver.stg_device_events",
                "silver.stg_fraud_alert_events",
            ],
        },
        {
            "job_id": "gold_model",
            "name": "gold_model",
            "description": "Build Gold dimensions, facts, and OBT tables for fraud monitoring.",
            "inputs": [
                "silver.stg_customers",
                "silver.stg_accounts",
                "silver.stg_cards",
                "silver.stg_merchants",
                "silver.stg_transactions",
                "silver.stg_login_events",
                "silver.stg_device_events",
                "silver.stg_fraud_alert_events",
            ],
            "outputs": [
                "gold.dim_customer",
                "gold.dim_account",
                "gold.dim_card",
                "gold.dim_merchant",
                "gold.dim_date",
                "gold.dim_channel",
                "gold.fact_transaction",
                "gold.fact_login_event",
                "gold.fact_device_event",
                "gold.fact_fraud_alert",
                "gold.obt_transaction_risk",
                "gold.obt_customer_risk_profile",
            ],
        },
        {
            "job_id": "compute_features",
            "name": "compute_features",
            "description": "Build customer and transaction-level feature tables for future fraud prediction.",
            "inputs": [
                "gold.fact_transaction",
                "gold.dim_customer",
                "gold.obt_transaction_risk",
            ],
            "outputs": [
                "gold.feat_customer_30d",
                "gold.feat_transaction_realtime",
                "gold.feat_customer_unified",
            ],
        },
        {
            "job_id": "publish_warehouse",
            "name": "publish_warehouse",
            "description": "Publish Gold tables and feature tables as DuckDB warehouse views.",
            "inputs": [
                "gold.dim_customer",
                "gold.fact_transaction",
                "gold.obt_transaction_risk",
                "gold.feat_customer_unified",
            ],
            "outputs": [
                "warehouse.finance_duckdb",
            ],
        },
    ]

    for job in jobs:
        job_urn = make_data_job_urn(
            orchestrator="airflow",
            flow_id="digital_banking_fraud_monitoring_pipeline",
            job_id=job["job_id"],
            cluster="local",
        )

        input_urns = [
            dataset_map[k]
            for k in job["inputs"]
            if k in dataset_map
        ]

        output_urns = [
            dataset_map[k]
            for k in job["outputs"]
            if k in dataset_map
        ]

        emit_data_job(
            emitter=emitter,
            job_urn=job_urn,
            name=job["name"],
            description=job["description"],
            input_urns=input_urns,
            output_urns=output_urns,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--server", default="http://localhost:8080")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    config = load_config(args.config)

    logging.info("Connecting to DataHub GMS: %s", args.server)
    emitter = DatahubRestEmitter(gms_server=args.server)

    logging.info("Emitting datasets, schemas, descriptions, ownership, and tags")
    dataset_map = emit_datasets(emitter, config)

    logging.info("Emitting dataset lineage")
    emit_dataset_lineage(emitter, dataset_map)

    logging.info("Emitting pipeline/data job lineage")
    emit_pipeline_jobs(emitter, dataset_map)

    logging.info("DataHub metadata emission completed successfully")


if __name__ == "__main__":
    main()