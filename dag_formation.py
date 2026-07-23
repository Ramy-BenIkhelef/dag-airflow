from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
 

test_variable = Variable.get("key_1")

with DAG(
    dag_id="dag_formation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["guide"],
) as dag:

    with TaskGroup(group_id="groupe_1") as groupe_1:

        hello = BashOperator(
            task_id="hello",
            bash_command="echo 'Airflow est prêt sur cette EC2'; hostname; date",
            # trigger_rule=TriggerRule.ONE_FAILED
        )
        ex_1 = BashOperator(
            task_id="ex_1",
            bash_command=f"echo '{test_variable}'"
        )

        [hello , ex_1]

    with TaskGroup(group_id="groupe_2") as groupe_2:  
        ex_2 = BashOperator(
            task_id="ex_2",
            bash_command="echo 'troisieme tache'"
        )

        ex_4 = BashOperator(
            task_id="ex_4",
            bash_command="exit 1"
        )
        [ex_2 , ex_4]

groupe_1 >> groupe_2 

