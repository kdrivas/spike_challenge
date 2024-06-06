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
    validate_data,
    preprocess_data,
)
from pipelines.music_model.model import (
    training_model,
)
training_model


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
    ###  Data validation 
    ###############################

    validation_step = PythonOperator(
        task_id=f"validate_data",
        python_callable=validate_data,
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

    ###############################
    ###  Model building
    ############################### 

    training_step = PythonOperator(
        task_id=f"training_model",
        python_callable=training_model,
        op_kwargs={
            "data_path": data_path, 
            "artifact_path": artifact_path, 
        }
    )

    (
        gather_step >>
        validation_step >>
        preprocessing_step >>
        training_step
    )
