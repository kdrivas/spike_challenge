"""
    Training pipeline for music model 
"""
import os
from datetime import datetime
import locale

from airflow import DAG
from airflow.operators.bash import BashOperator


locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


with DAG(
    dag_id=f"batch_prediction_v1_0",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["batch_prediction"],
) as dag:

    validate_assets_task = BashOperator(
        task_id="validate_assets",
        bash_command=f"python -m model validate_assets --base_path {AIRFLOW_HOME}",
    )

    collect_assets_task = BashOperator(
        task_id="collect_assets",
        bash_command=f"python -m model collect_assets --base_path {AIRFLOW_HOME}",
    )

    create_batch_data_task = BashOperator(
        task_id="create_batch_data",
        bash_command=f"python -m model create_batch_data --base_path {AIRFLOW_HOME}",
    )

    run_predictions_task = BashOperator(
        task_id="run_predictions",
        bash_command=f"python -m model run_batch_prediction --base_path {AIRFLOW_HOME}",
    )

    (
        validate_assets_task >>
        collect_assets_task >>
        create_batch_data_task >>
        run_predictions_task
    )
