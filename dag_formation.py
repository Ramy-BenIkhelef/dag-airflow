from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
 
with DAG(
    dag_id="dag_formation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["guide"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Airflow est prêt sur cette EC2'; hostname; date",
        trigger_rule=TriggerRule.ONE_FAILED
    )
    ex_1 = BashOperator(
        task_id="ex_1",
        bash_command="sleep 10"
    )   
    ex_2 = BashOperator(
        task_id="ex_2",
        bash_command="echo 'troisieme tache'"
    )

    ex_4 = BashOperator(
        task_id="ex_4",
        bash_command="exit 1"
    )

ex_1 >> ex_4 >> [hello , ex_2]