from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensor
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

SSH_CONN = "serveur_ssh"
POOL = "chaines_exclusives"
START = pendulum.datetime(2025, 1, 1, tz="Europe/Paris")

@dag(
    dag_id="chaine1",
    start_date=START,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def chaine_1():
    a = SSHOperator(
        task_id="Job_A",
        ssh_conn_id=SSH_CONN,
        command="/opt/jobs/job_a.sh",
        cmd_timeout=None,
    )

    attendre_2h = DateTimeSensor(
        task_id="attendre_2h",
        target_time="{{ dag_run.conf['heure_2'] }}",
        mode="reschedule",
        poke_interval=30,
    )

    b = SSHOperator(
        task_id="Job_B",
        ssh_conn_id=SSH_CONN,
        command="/opt/jobs/job_b.sh",
        cmd_timeout=None,
    )

    attendre_3h = DateTimeSensor(
        task_id="attendre_3h",
        target_time="{{ dag_run.conf['heure_3'] }}",
        mode="reschedule",
        poke_interval=30,
    )

    c = SSHOperator(
        task_id="Job_C",
        ssh_conn_id=SSH_CONN,
        command="/opt/jobs/job_c.sh",
        cmd_timeout=None,
    )

    a >> attendre_2h >> b >> attendre_3h >> c



@dag(
    dag_id="chaine2",
    start_date=START,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def chaine_2():
    SSHOperator(
        task_id="Job_D",
        ssh_conn_id=SSH_CONN,
        command="/opt/jobs/job_d.sh",
        cmd_timeout=None,
    )



@dag(
    dag_id="pilotage",
    start_date=START,
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
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

    @task.sensor(
        poke_interval=60,
        timeout=timedelta(hours=23),
        mode="reschedule",
        soft_fail=True,
    )
    def attendre_fichier():
        # Vérification sur le serveur distant, pas sur le worker Airflow.
        with SSHHook(ssh_conn_id=SSH_CONN).get_conn() as client:
            _, stdout, stderr = client.exec_command(
                "find /data/entree -maxdepth 1 -type f "
                "-name '*.csv' -print -quit"
            )
            resultat = stdout.read().decode().strip()
            erreur = stderr.read().decode()
            code = stdout.channel.recv_exit_status()

            if code != 0:
                raise RuntimeError(f"Vérification SSH échouée : {erreur}")

            return bool(resultat)

    lancer_1 = TriggerDagRunOperator(
        task_id="lancer_chaine_1",
        trigger_dag_id="chaine_1",
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
        trigger_dag_id="chaine_2",
        pool=POOL,
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    attendre_1h >> lancer_1
    attendre_fichier() >> lancer_2


chaine_1()
chaine_2()
pilotage_chaines()