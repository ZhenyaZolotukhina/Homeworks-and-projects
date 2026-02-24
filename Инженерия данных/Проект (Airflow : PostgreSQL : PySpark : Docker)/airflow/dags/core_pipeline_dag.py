from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

AIRFLOW_HOME = "/opt/airflow"

with DAG(
    dag_id="core_delivery_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["deliveries", "core"],
) as dag:

    load_core = BashOperator(
        task_id="load_core_tables",
        bash_command=(
            f"python {AIRFLOW_HOME}/scripts/load_core.py"
        ),
    )

    build_mart_orders = BashOperator(
        task_id="build_mart_orders",
        bash_command=(
            f"python {AIRFLOW_HOME}/scripts/data_mart1.py"
        ),
    )

    build_mart_items = BashOperator(
        task_id="build_mart_items",
        bash_command=(
            f"python {AIRFLOW_HOME}/scripts/data_mart_items.py"
        ),
    )

    load_core >> [build_mart_orders, build_mart_items]
