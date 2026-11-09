import pendulum

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.providers.standard.sensors.date_time import DateTimeSensor
from airflow.providers.standard.sensors.filesystem import FileSensor

POOL = "chaines_exclusives"
START = pendulum.datetime(2025, 1, 1, tz="Europe/Paris")


@dag(
    dag_id="chaine1",
    start_date=START,
    schedule=None,
    catchup=False,
    tags=['complexe', 'shedule_complex'],
    max_active_runs=1,
)
def chaine_1():
    a = BashOperator(
        task_id="Job_A",
        bash_command="bash /opt/jobs/job_a.sh ",
        skip_on_exit_code=None,
    )

    attendre_2h = DateTimeSensor(
        task_id="attendre_2h",
        target_time="{{ dag_run.conf['heure_2'] }}",
        mode="reschedule",
        poke_interval=30,
    )

    b = BashOperator(
        task_id="Job_B",
        bash_command="bash /opt/jobs/job_b.sh ",
        skip_on_exit_code=None,
    )

    attendre_3h = DateTimeSensor(
        task_id="attendre_3h",
        target_time="{{ dag_run.conf['heure_3'] }}",
        mode="reschedule",
        poke_interval=30,
    )

    c = BashOperator(
        task_id="Job_C",
        bash_command="bash /opt/jobs/job_c.sh ",
        skip_on_exit_code=None,
    )

    a >> attendre_2h >> b >> attendre_3h >> c


@dag(
    dag_id="chaine2",
    start_date=START,
    schedule=None,
    catchup=False,
    tags=['complexe', 'shedule_complex'],
    max_active_runs=1,
)
def chaine_2():
    BashOperator(
        task_id="Job_D",
        bash_command="bash /opt/jobs/job_d.sh ",
        skip_on_exit_code=None,
    )


@dag(
    dag_id="pilotage",
    start_date=START,
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=['complexe', 'shedule_complex'],
)
def pilotage_chaines():
    attendre_1h = DateTimeSensor(
        task_id="attendre_1h",
        target_time=(
            "{{ data_interval_end.in_timezone('Europe/Paris')"
            ".replace(hour=1, minute=0, second=0).isoformat() }}"
        ),
        mode="reschedule",
        poke_interval=30,
    )

    attendre_fichier = FileSensor(
        task_id="attendre_fichier",
        fs_conn_id="fs_default",
        filepath="/data/entree/*.csv",
        mode="reschedule",
        poke_interval=60,
        timeout=23 * 3600,
        soft_fail=True,
    )

    lancer_1 = TriggerDagRunOperator(
        task_id="lancer_chaine_1",
        trigger_dag_id="chaine1",
        conf={
            "heure_2": (
                "{{ data_interval_end.in_timezone('Europe/Paris')"
                ".replace(hour=2, minute=0, second=0).isoformat() }}"
            ),
            "heure_3": (
                "{{ data_interval_end.in_timezone('Europe/Paris')"
                ".replace(hour=3, minute=0, second=0).isoformat() }}"
            ),
        },
        pool=POOL,
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    lancer_2 = TriggerDagRunOperator(
        task_id="lancer_chaine_2",
        trigger_dag_id="chaine2",
        pool=POOL,
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    attendre_1h >> lancer_1
    attendre_fichier >> lancer_2


chaine_1()
chaine_2()
pilotage_chaines()