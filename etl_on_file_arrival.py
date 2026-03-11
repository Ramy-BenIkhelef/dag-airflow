from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

with DAG(
    dag_id="etl_on_file_arrival",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lab", "etl", "sensor"]
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/airflow_lab/input/customers.csv",
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    extract = BashOperator(
        task_id="extract",
        bash_command="""
        echo "Lecture du fichier source"
        cat /tmp/airflow_lab/input/customers.csv
        """
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="""
        mkdir -p /tmp/airflow_lab/work
        awk 'NR==1{print $0",country"; next} {print $0",FRANCE"}' /tmp/airflow_lab/input/customers.csv > /tmp/airflow_lab/work/customers_transformed.csv
        echo "Fichier transformé :"
        cat /tmp/airflow_lab/work/customers_transformed.csv
        """
    )

    load = BashOperator(
        task_id="load",
        bash_command="""
        mkdir -p /tmp/airflow_lab/output
        cp /tmp/airflow_lab/work/customers_transformed.csv /tmp/airflow_lab/output/customers_loaded.csv
        echo "Chargement terminé"
        """
    )

    generate_report = BashOperator(
        task_id="generate_report",
        bash_command="""
        mkdir -p /tmp/airflow_lab/output
        cat > /tmp/airflow_lab/output/report.txt <<EOF
Pipeline ETL terminé avec succès.
Fichier source : /tmp/airflow_lab/input/customers.csv
Fichier chargé : /tmp/airflow_lab/output/customers_loaded.csv
EOF
        echo "Rapport généré"
        cat /tmp/airflow_lab/output/report.txt
        """
    )

    wait_for_file >> extract >> transform >> load >> generate_report
