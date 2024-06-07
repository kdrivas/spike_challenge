"""
    Training pipeline for milk model 
"""
import os
from datetime import datetime
import locale

from airflow import DAG
from airflow.operators.bash import BashOperator


locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


with DAG(
    dag_id=f"music_model_v1_0",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training_pipeline"],
) as dag:

    validate_assets_task = BashOperator(
        task_id="validate_assets",
        bash_command=f"python -m model validate_assets --base_path {AIRFLOW_HOME}",
    )

    collect_assets_task = BashOperator(
        task_id="collect_assets",
        bash_command=f"python -m model validate_assets --base_path {AIRFLOW_HOME}",
    )

    preprocessing_task = BashOperator(
        task_id="preprocessing",
        bash_command=f"python -m model preprocess_assets --base_path {AIRFLOW_HOME}",
    )

    training_model_task = BashOperator(
        task_id="training_model",
        bash_command=f"python -m model training_model --base_path {AIRFLOW_HOME}",
    )

    (
        validate_assets_task >>
        collect_assets_task >>
        preprocessing_task >>
        training_model_task
    )
