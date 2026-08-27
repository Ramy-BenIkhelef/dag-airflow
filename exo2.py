from datetime import datetime
from airflow.sdk import DAG, Asset
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

mon_fichier = Asset("file:///tmp/mon_asset.txt")

with DAG(
    dag_id="producteur_asset_bash",
    start_date=datetime(2026, 8, 27),
    schedule=None,
    catchup=False,
    tags=["exo2"],
) as dag_producteur: 
    tache1 = BashOperator(
        task_id="produire_fichier",
        bash_command="echo 'hello' > /tmp/mon_asset.txt",
        outlets=[mon_fichier]
    )

    with TaskGroup(group_id="groupe_1") as echo_donne:

        tache2 = BashOperator(
            task_id="tache2",
            bash_command="echo 'ma tache 2'",
        )
        tache4 = BashOperator(
            task_id="tache4",
            bash_command="echo 'ma tache 4'",
            trigger_rule="all_failed"
        )
       
    tache2 >> tache4
        
tache1 >> echo_donne

with DAG(
    dag_id="consommateur_asset_bash",
    start_date=datetime(2026, 8, 27),
    schedule=[mon_fichier],
    catchup=False,
    tags=["exo2"],
) as dag_consommateur: 
    tache3 = BashOperator(
        task_id="lire_fichier",
        bash_command="cat /tmp/mon_asset.txt",
    )
