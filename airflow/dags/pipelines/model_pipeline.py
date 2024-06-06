from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="training_dag"
) as dag:

    BashOperator(
        task_id="bash_task",
        bash_command='echo "Hello world"',
    )
