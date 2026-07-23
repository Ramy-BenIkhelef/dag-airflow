from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
 
with DAG(
    dag_id="dag_formation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["guide"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Airflow est prêt sur cette EC2'; hostname; date"
    )

    ex_1 = BashOperator(
        task_id="ex_1",
        bash_command="echo 'deuxième tache"
    )   
    ex_2 = BashOperator(
        task_id="ex_2",
        bash_command="echo 'troisieme tache"
    )

ex_1 >> hello >> ex_2