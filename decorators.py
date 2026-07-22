from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id="decorator_dag",
    start_date=datetime(2026, 2, 1),
    schedule="0 * * * *",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
    tags=["decorator_dag", "formation", "data_eng"],
    description="A simple DAG to demonstrate decorator usage",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=2,
)
def decorator_dag():

    @task
    def task_a():
        print("Task A is running")
        return 10

    @task
    def task_b(task_a_result):
        print("Task B is running")
        print(f"Task A result: {task_a_result}")

    @task
    def task_c():
        print("Task C is running")
        return "Task C result"

    task_a_result = task_a()
    task_b_task = task_b(task_a_result)
    task_c_task = task_c()

    task_b_task >> task_c_task


decorator_dag()
