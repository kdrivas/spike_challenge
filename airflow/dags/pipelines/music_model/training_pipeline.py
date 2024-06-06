"""
    Training pipeline for milk model 
"""
import os
from datetime import datetime
import locale

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.config import VERSION
from pipelines.music_model.data import (
    collect_music,
    preprocess_data
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
    artifact_path = os.path.join(AIRFLOW_HOME, "artifacts")

    gather_step = PythonOperator(
        task_id=f"gather_reggaeton_music",
        python_callable=collect_music,
        op_kwargs={
            "path": data_path, 
        }
    )

    ###############################
    ###  Preprocessing data 
    ###############################

    preprocessing_step = PythonOperator(
        task_id=f"preprocess_data",
        python_callable=preprocess_data,
        op_kwargs={
            "data_path": data_path, 
            "artifact_path": artifact_path, 
        }
    )

    gather_step >> preprocessing_step
