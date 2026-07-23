from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sdk.execution_time.xcom import XCom

def fonction_ex_1(ti):
    test_variable = Variable.get("key_1")
    ti.xcom_push(key='ex_1', value="ma clé xcom")

def fonction_ex_2(ti):
    valeur = ti.xcom_pull(task_ids='groupe_1.ex_1' , key='ex_1')
    print(f"Valeur reçue depuis XCom : {valeur}")

with DAG(
    dag_id="dag_formation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["guide"],
) as dag:

    with TaskGroup(group_id="groupe_1") as groupe_1:

        ex_1 = PythonOperator(
            task_id="ex_1",
            python_callable=fonction_ex_1
        )

    with TaskGroup(group_id="groupe_2") as groupe_2:

        ex_2 = PythonOperator(
            task_id="ex_2",
            python_callable=fonction_ex_2
        )

    groupe_1 >> groupe_2

