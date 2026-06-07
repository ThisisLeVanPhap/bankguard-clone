from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/airflow/project"


with DAG(
    dag_id="digital_banking_fraud_monitoring_pipeline",
    description="End-to-end digital banking fraud monitoring data pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["coursework", "banking", "fraud", "delta", "datahub"],
) as dag:

    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/generate_data.py --config configs/generator_config.yaml"
        ),
    )

    run_section_02_pipeline = BashOperator(
        task_id="run_section_02_pipeline",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/run_02_pipeline.py --config configs/pipeline_config.yaml"
        ),
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/quality_checks.py --config configs/pipeline_config.yaml"
        ),
    )

    emit_datahub_metadata = BashOperator(
        task_id="emit_datahub_metadata",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python src/emit_datahub_metadata.py "
            "--config configs/pipeline_config.yaml "
            "--server http://host.docker.internal:8080"
        ),
    )

    generate_source_data >> run_section_02_pipeline >> run_quality_checks >> emit_datahub_metadata