from datetime import datetime
from airflow.sdk import DAG, Asset
from airflow.operators.bash import BashOperator

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

with DAG(
    dag_id="consommateur_asset_bash",
    start_date=datetime(2026, 8, 27),
    schedule=[mon_fichier],
    catchup=False,
    tags=["exo2"],
) as dag_consommateur: 
    tache2 = BashOperator(
        task_id="lire_fichier",
        bash_command="cat /tmp/mon_asset.txt",
    )
