# airflow/dags/provider_trust_dag.py
# Orchestrates the ProviderTrust pipeline as one automated workflow:
#   bronze ingest -> silver transform -> entity resolution -> quality scoring
# Each task runs an existing pipeline script. Tasks run in order; if one
# fails, downstream tasks are skipped so bad data never propagates.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default behavior applied to every task in this DAG.
default_args = {
    "owner": "data-engineering",
    "retries": 1,                          # retry once on transient failure
    "retry_delay": timedelta(minutes=1),
}

# The project is mounted at /opt/app inside the Airflow container.
# We run each script with the container's Python from that directory.
# Run each script INSIDE the Spark container (which has Java + connectors).
# Airflow orchestrates; Spark executes. This is the orchestrator/compute split.
RUN = "docker exec spark python3 /opt/app/pipeline"

with DAG(
    dag_id="provider_trust_pipeline",
    description="Provider Data Management: ingest, clean, resolve, score",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),       # a past date so it can run immediately
    schedule=None,                          # manual trigger only (no auto-schedule)
    catchup=False,                          # don't backfill past dates
    tags=["healthcare", "pdm", "spark"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"{RUN}/bronze_ingest.py",
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command=f"{RUN}/silver_transform.py",
    )

    resolution = BashOperator(
        task_id="entity_resolution",
        bash_command=f"{RUN}/gold_entity_resolution.py",
    )

    scoring = BashOperator(
        task_id="quality_scoring",
        bash_command=f"{RUN}/gold_quality_scoring.py",
    )

    # Define the order: each must succeed before the next begins.
    bronze >> silver >> resolution >> scoring