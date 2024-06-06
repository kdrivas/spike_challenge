"""
    Training pipeline for milk model 
"""
import os
from datetime import datetime
import locale

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines.music_model.constants import VERSION
from pipelines.music_model.data import (
    collect_music
)


locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


with DAG(
    dag_id=f"music_model_v{VERSION}",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training_pipeline"],
) as dag:

    
    ###############################
    ## Data Gathering 
    ###############################

    data_path = os.path.join(AIRFLOW_HOME, "data")

    gather_milk_step = PythonOperator(
        task_id=f"gather_reggaeton_music",
        python_callable=collect_music,
        op_kwargs={
            "path": data_path, 
        }
    )
