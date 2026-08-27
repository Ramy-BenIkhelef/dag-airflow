from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="exemple_xcom_bash",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    produire_xcom = BashOperator(
        task_id="produire_xcom",
        bash_command='echo "Bonjour depuis Bash"',
        do_xcom_push=True,
    )

    consommer_xcom = BashOperator(
        task_id="consommer_xcom",
        bash_command='echo "Message reçu : {{ ti.xcom_pull(task_ids=\'produire_xcom\') }}"',
    )

    produire_xcom >> consommer_xcom
