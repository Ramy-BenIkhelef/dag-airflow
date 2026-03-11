from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="generate_input_file",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lab", "producer"]
) as dag:

    create_file = BashOperator(
        task_id="create_file",
        bash_command="""
        mkdir -p /tmp/airflow_lab/input
        cat > /tmp/airflow_lab/input/customers.csv <<EOF
id,name,city
1,Alice,Paris
2,Bob,Lyon
3,Charlie,Marseille
EOF
        echo "Fichier généré avec succès"
        """
    )
