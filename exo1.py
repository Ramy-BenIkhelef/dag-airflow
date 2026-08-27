from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 8, 27),
    schedule=None,
    catchup=False,
    tags=["exo1"],
) as dag: 
    tache1 = BashOperator(
        task_id="tache1",
        bash_command="echo 'Bonjour depuis Airflow'",
    )

    tache2 = BashOperator(
        task_id="tache2",
        bash_command="""
            echo "Valeur Tache1 depuis XCom : {{ ti.xcom_pull(task_ids='tache1') }}"
        """,
    ) 

    tache1 >> tache2   